"""
Testsuite for dataset client. Requires access to a dataset instance using
the globusonline webauthn2 provider and a user that has admin rights.

Can be run with nose like this:
    export DATASET_CLIENT_TEST_GOAUTH_TOKEN=$(cat /path/to/token)
    export DATASET_CLIENT_TEST_BASE_URL="https://localhost/service/dataset"
    cd /path/to/src
    nosetests -v
"""
import os
import uuid
import unittest

from globusonline.catalog.client import dataset_client
from globusonline.catalog.client.rest_client import RestClientError


class TestDatasetClient(unittest.TestCase):
    """Test the client by creating a test catalog (named by uuid to avoid
    conflicts) and creating, searching, deleting subjects and tags within
    that test catalog. Attempts to delete the catalog on cleanup, unless the
    env var DATASET_CLIENT_TEST_NO_DELETE is set to aid debugging, in which
    case it will print the name and id."""
    @classmethod
    def setUpClass(cls):
        goauth_token = os.environ.get("DATASET_CLIENT_TEST_GOAUTH_TOKEN")
        base_url = os.environ.get("DATASET_CLIENT_TEST_BASE_URL",
                                  dataset_client.DEFAULT_BASE_URL)
        no_delete = os.environ.get("DATASET_CLIENT_TEST_NO_DELETE")
        cls.no_delete = bool(no_delete)
        if goauth_token is None:
            raise Exception("Missing required environment variable "
                           +"DATASET_CLIENT_TEST_GOAUTH_TOKEN")
        print "base_url:", base_url
        cls.client = dataset_client.DatasetClient(goauth_token,
                                                  base_url=base_url)

        cls.run_id = uuid.uuid1()
        cls.catalog_name = "test_%s" % cls.run_id
        r, data = cls.client.create_catalog(name=cls.catalog_name,
                            description="TestDatasetClient %s" % cls.run_id)
        cls.catalog_id = data["id"]
        print "Created catalog: %s (%d)" % (cls.catalog_name, cls.catalog_id)

    def _check_dataset_create_delete(self, name):
        r, data = self.client.create_dataset(self.catalog_id,
                                             { "name": name })
        self.assertIn("id", data)
        dataset_id = data["id"]
        r, data = self.client.get_datasets(self.catalog_id)
        found = False
        for ds in data:
            if ds["id"] == dataset_id:
                found = True
                break
        self.assertTrue(found)
        self.assertEqual(ds["name"], name)
        self.client.delete_dataset(self.catalog_id, dataset_id)
        r, data = self.client.get_datasets(self.catalog_id)
        found = False
        for ds in data:
            if ds["id"] == dataset_id:
                found = True
                break
        self.assertFalse(found)

    def test_dataset_ascii_nospecial(self):
        self._check_dataset_create_delete("test1")

    def test_dataset_space(self):
        self._check_dataset_create_delete("dataset space in name")

    def test_dataset_slash(self):
        self._check_dataset_create_delete("dataset/slash/in/name")

    def test_dataset_special(self):
        self._check_dataset_create_delete(
                    "dataset?with.:;'\"+=special[]{}()*&#$@%!~`")

    def test_dataset_unicode(self):
        self._check_dataset_create_delete(u"dataset\u0101")

    def _create_dataset(self, name, num_members):
        _, data = self.client.create_dataset(self.catalog_id,
                                             { "name": name })
        dataset_id = data["id"]
        for i in xrange(num_members):
            data_type = ("file", "directory")[i % 2]
            data_uri = "/%s/member%d" % (name, i)
            self.client.create_members(self.catalog_id, dataset_id,
                                       [dict(data_type=data_type,
                                             data_uri=data_uri)])
        return dataset_id

    def test_member(self):
        ds1_id = self._create_dataset("ds1", 13)
        ds2_id = self._create_dataset("ds2", 17)
        _, data = self.client.get_members(self.catalog_id, ds1_id)
        self.assertEqual(len(data), 13)

        _, data = self.client.get_members(self.catalog_id, ds2_id)
        self.assertEqual(len(data), 17)

        ds2_member1_id = data[0]["id"]
        self.client.delete_member(self.catalog_id, ds2_id, ds2_member1_id)

        _, data = self.client.get_members(self.catalog_id, ds2_id)
        self.assertEqual(len(data), 16)

    def test_annotation_def(self):
        _, data = self.client.get_annotation_defs(self.catalog_id)
        len1 = len(data)
        names1 = set(d["name"] for d in data)
        # Check for some pre-defined tags
        self.assertIn("id", names1)
        self.assertIn("data_uri", names1)
        self.assertIn("data_type", names1)

        # Add some defs
        self.client.create_annotation_def(self.catalog_id, "deftesttext1",
                                          "text")
        self.client.create_annotation_def(self.catalog_id, "deftestint1",
                                          "int8", True, False)
        _, data = self.client.get_annotation_defs(self.catalog_id)
        len2 = len(data)
        names2 = set(d["name"] for d in data)
        self.assertEqual(names2, names1 | set(["deftesttext1", "deftestint1"]))

    def test_annotation_def_space(self):
        _, data = self.client.get_annotation_defs(self.catalog_id)
        len1 = len(data)
        names1 = set(d["name"] for d in data)
        # Check for some pre-defined tags
        self.assertIn("id", names1)
        self.assertIn("data_uri", names1)
        self.assertIn("data_type", names1)

        # Add some defs
        self.client.create_annotation_def(self.catalog_id, "deftest space1",
                                          "text")
        _, data = self.client.get_annotation_defs(self.catalog_id)
        len2 = len(data)
        names2 = set(d["name"] for d in data)
        self.assertEqual(names2, names1 | set(["deftest space1"]))

    def test_dataset_annotation(self):
        self.client.create_annotation_def(self.catalog_id, "testdstext1",
                                          "text")
        self.client.create_annotation_def(self.catalog_id, "testdsint1", "int8")
        r, data = self.client.create_dataset(self.catalog_id,
                                             { "name":
                                                   "test_dataset_annotation" })
        dataset_id = data["id"]
        # TODO: you have to specify which tags you want, or it only
        # returns id and owner. Probably not the behavior we want! Also
        # it returns None values for not present tags.
        annotation_names = ["id", "testdstext1", "testdsint1"]
        _, data = self.client.get_dataset_annotations(self.catalog_id,
                                                      dataset_id,
                                                      annotation_names)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["testdstext1"], None)
        self.assertEqual(data[0]["testdsint1"], None)

        annotations = dict(testdstext1="abc123", testdsint1=123)
        self.client.add_dataset_annotations(self.catalog_id,
                                            dataset_id, annotations)
        _, data = self.client.get_dataset_annotations(self.catalog_id,
                                                      dataset_id,
                                                      annotation_names)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["testdstext1"], "abc123")
        self.assertEqual(data[0]["testdsint1"], 123)

        # test delete
        self.client.delete_dataset_annotation(self.catalog_id, dataset_id,
                                              "testdsint1")
        _, data = self.client.get_dataset_annotations(self.catalog_id,
                                                      dataset_id,
                                                      annotation_names)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["testdstext1"], "abc123")
        self.assertEqual(data[0]["testdsint1"], None)

    def test_dataset_annotation_space(self):
        self.client.create_annotation_def(self.catalog_id, "testds space1",
                                          "text")
        r, data = self.client.create_dataset(self.catalog_id,
                                 { "name": "test_dataset_annotation_space" })
        dataset_id = data["id"]
        annotation_names = ["id", "testds space1"]
        _, data = self.client.get_dataset_annotations(self.catalog_id,
                                                      dataset_id,
                                                      annotation_names)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["testds space1"], None)

        annotations = { "testds space1": "abc123" }
        self.client.add_dataset_annotations(self.catalog_id,
                                            dataset_id, annotations)
        _, data = self.client.get_dataset_annotations(self.catalog_id,
                                                      dataset_id,
                                                      annotation_names)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["testds space1"], "abc123")

        # test delete
        self.client.delete_dataset_annotation(self.catalog_id, dataset_id,
                                              "testds space1")
        _, data = self.client.get_dataset_annotations(self.catalog_id,
                                                      dataset_id,
                                                      annotation_names)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["testds space1"], None)

    def test_member_annotation(self):
        self.client.create_annotation_def(self.catalog_id,
                                          "testmtext1", "text")
        self.client.create_annotation_def(self.catalog_id,
                                          "testmint1", "int8")
        r, data = self.client.create_dataset(self.catalog_id,
                                             { "name":
                                                   "test_member_annotation" })
        dataset_id = data["id"]

        self.client.create_members(self.catalog_id, dataset_id,
                                   [dict(data_type="file", data_uri="/file"),
                                    dict(data_type="directory",
                                         data_uri="/dir")])

        # Since create does not return ids, we have to make another call
        # to get the ids
        r, data = self.client.get_members(self.catalog_id, dataset_id)
        for m in data:
            data_uri = m["data_uri"]
            if data_uri == "/dir":
                dir_id = m["id"]
            elif data_uri == "/file":
                file_id = m["id"]
            else:
                assert False

        annotation_names = ["id", "testmtext1", "testmint1"]

        # update file member annotations
        _, file_data = self.client.get_member_annotations(self.catalog_id,
                                                          dataset_id,
                                                          file_id,
                                                          annotation_names)
        self.assertEqual(len(file_data), 1)
        self.assertEqual(file_data[0], dict(testmtext1=None,
                                           testmint1=None,
                                           id=file_id))

        annotations = dict(testmtext1="abc123", testmint1=123)
        self.client.add_member_annotations(self.catalog_id,
                                           dataset_id, file_id, annotations)
        _, file_data = self.client.get_member_annotations(self.catalog_id,
                                                          dataset_id,
                                                          file_id,
                                                          annotation_names)
        self.assertEqual(len(file_data), 1)
        self.assertEqual(file_data[0], dict(testmtext1="abc123",
                                            testmint1=123,
                                            id=file_id))

        # update dir member annotations
        _, dir_data = self.client.get_member_annotations(self.catalog_id,
                                                         dataset_id,
                                                         dir_id,
                                                         annotation_names)
        self.assertEqual(len(dir_data), 1)
        self.assertEqual(dir_data[0], dict(testmtext1=None,
                                           testmint1=None,
                                           id=dir_id))

        annotations = dict(testmtext1="abc456", testmint1=456)
        self.client.add_member_annotations(self.catalog_id,
                                           dataset_id, dir_id, annotations)
        _, dir_data = self.client.get_member_annotations(self.catalog_id,
                                                         dataset_id,
                                                         dir_id,
                                                         annotation_names)
        self.assertEqual(len(dir_data), 1)
        self.assertEqual(dir_data[0], dict(testmtext1="abc456",
                                           testmint1=456,
                                           id=dir_id))

        # verify that file member is unchanged
        _, file_data = self.client.get_member_annotations(self.catalog_id,
                                                          dataset_id,
                                                          file_id,
                                                          annotation_names)
        self.assertEqual(len(file_data), 1)
        self.assertEqual(file_data[0], dict(testmtext1="abc123",
                                            testmint1=123,
                                            id=file_id))

        # verify that dataset is unchanged
        _, ds_data = self.client.get_dataset_annotations(self.catalog_id,
                                                         dataset_id,
                                                         annotation_names)
        self.assertEqual(len(ds_data), 1)
        self.assertEqual(ds_data[0], dict(testmtext1=None,
                                          testmint1=None,
                                          id=dataset_id))

        # delete one file annotation
        self.client.delete_member_annotation(self.catalog_id,
                                             dataset_id, file_id, "testmint1")
        _, file_data = self.client.get_member_annotations(self.catalog_id,
                                                          dataset_id,
                                                          file_id,
                                                          annotation_names)
        self.assertEqual(len(file_data), 1)
        self.assertEqual(file_data[0], dict(testmtext1="abc123",
                                            testmint1=None,
                                            id=file_id))

    def test_member_annotation_space(self):
        self.client.create_annotation_def(self.catalog_id,
                                          "testm space1", "text")
        r, data = self.client.create_dataset(self.catalog_id,
                         { "name": "test_member_annotation_space" })
        dataset_id = data["id"]

        self.client.create_members(self.catalog_id, dataset_id,
                                   [dict(data_type="file", data_uri="/file")])

        # Since create does not return ids, we have to make another call
        # to get the ids
        r, data = self.client.get_members(self.catalog_id, dataset_id)
        for m in data:
            data_uri = m["data_uri"]
            if data_uri == "/dir":
                dir_id = m["id"]
            elif data_uri == "/file":
                file_id = m["id"]
            else:
                assert False

        annotation_names = ["id", "testm space1"]

        # update file member annotations
        _, file_data = self.client.get_member_annotations(self.catalog_id,
                                                          dataset_id,
                                                          file_id,
                                                          annotation_names)
        self.assertEqual(len(file_data), 1)
        self.assertEqual(file_data[0], { "testm space1": None,
                                         "id": file_id })

        annotations = { "testm space1": "abc123" }
        self.client.add_member_annotations(self.catalog_id,
                                           dataset_id, file_id, annotations)
        _, file_data = self.client.get_member_annotations(self.catalog_id,
                                                          dataset_id,
                                                          file_id,
                                                          annotation_names)
        self.assertEqual(len(file_data), 1)
        self.assertEqual(file_data[0],  { "testm space1": "abc123",
                                          "id": file_id })

        # delete file annotation
        self.client.delete_member_annotation(self.catalog_id, dataset_id,
                                             file_id, "testm space1")
        _, file_data = self.client.get_member_annotations(self.catalog_id,
                                                          dataset_id,
                                                          file_id,
                                                          annotation_names)
        self.assertEqual(len(file_data), 1)
        self.assertEqual(file_data[0], { "testm space1": None,
                                         "id": file_id })

    def test_error_dataset_tag_not_found(self):
        _, data = self.client.create_dataset(self.catalog_id,
                        { "name": "test_error_dataset_tag_not_found" })
        dataset_id = data["id"]
        try:
            self.client.add_dataset_annotations(self.catalog_id, dataset_id,
                                                { "doesnotexist": "abc123" })
        except RestClientError as e:
            print "got exc", e
            self.assertEqual(e.response.status, 409)
            self.assertIn('doesnotexist" not defined', e.message)
        else:
            assert False, "expected error, got success"

    @classmethod
    def tearDownClass(cls):
        if cls.no_delete:
            print "Not deleting catalog %s (%d)" % (cls.catalog_name,
                                                    cls.catalog_id)
        else:
            cls.client.delete_catalog(cls.catalog_id)
