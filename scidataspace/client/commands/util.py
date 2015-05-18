UNDEFINED = "undefined"

class SafeList(list):
    def get(self, index, default=None):
        return self[index] if len(self) > index else default