import os
import re
import readline
import subprocess
import logging

gd_commands = ['foo', 'bar']
RE_SPACE = re.compile('.*\s+$', re.M)


LOG_FILENAME = '/tmp/completer.log'
if not os.path.isfile(LOG_FILENAME):
    with open(LOG_FILENAME, "w") as f:
        f.write('x\n')
    os.chmod(LOG_FILENAME, 0o777)
logging.basicConfig(filename=LOG_FILENAME,
                    level=logging.DEBUG,
                    )

class BufferAwareCompleter(object):
    # builtins, gd_commands and lookup:
    # - key: command
    # - value: True if path argument
    def __init__(self, commands, special_char):
        self.special_cmd = special_char
        self.options = commands
        self.current_candidates = []

        self.builtins = None
        self.gd_commands = {self.special_cmd+k:v for k,v in commands.items()}

        self.cmd_lookup = None
        self.path_lookup = None

        # try to get shell builtins
        shell = os.environ.get('SHELL')
        if shell:
            p = subprocess.Popen([shell, '-c', 'compgen -b'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            self.builtins = dict((key, {}) for key in out.split())
        if not self.builtins:
            self.builtins = { 'cd': {} }

    def _generate(self, prefix):
        begins_with = lambda s, p=prefix: s[:len(prefix)] == prefix
        self.cmd_lookup = {}

        # append gd_commands in PATH
        for path in map(os.path.expanduser, os.environ.get('PATH', '').split(':')):
            if not os.path.isdir(path):
                continue

            for f in filter(begins_with, os.listdir(path)):
                self.cmd_lookup[f] = True

        # append shell builtins
        for n in filter(begins_with, self.builtins.keys()):
            self.cmd_lookup[n] = self.builtins[n]

        # append gd_commands
        for n in filter(begins_with,self.gd_commands.keys()):
            self.cmd_lookup[n] = self.gd_commands[n]

        #print "\nGenerated gd_commands for prefix %s: %s" % (`prefix`, `self.cmd_lookup`)

    def _listdir(self, root):
        "List directory 'root' appending the path separator to subdirs."
        res = []
        for name in os.listdir(root):
            path = os.path.join(root, name)
            if os.path.isdir(path):
                name += os.sep
            res.append(name)
        return res

    def _complete_path(self, path=None):
        "Perform completion of filesystem path."
        if not path:
            return self._listdir('.')
        dirname, rest = os.path.split(path)
        tmp = dirname if dirname else '.'
        res = [os.path.join(dirname, p)
                for p in self._listdir(tmp) if p.startswith(rest)]
        # more than one match, or single match which does not exist (typo)
        if len(res) > 1 or not os.path.exists(path):
            return res
        # resolved to a single directory, so return list of files below it
        if os.path.isdir(path):
            return [os.path.join(path, p) for p in self._listdir(path)]
        # exact file match terminates this completion
        return [path + ' ']

    def complete_path(self, args):
        "Path completion."
        if not args:
            return self._complete_path('.')
        # treat the last arg as a path and complete it
        return self._complete_path(args[-1])

    def complete(self, text, state):
        "Generic readline completion entry point."
        buffer = readline.get_line_buffer()
        words = buffer.split()
        logging.debug('words=%s', words)

        # account for last argument ending in a space
        if words and RE_SPACE.match(buffer):
            words.append('')

        # command completion
        if len(words) < 2:
            if state == 0:
                self._generate(text)
            return ([c + ' ' for c in self.cmd_lookup.keys()] + [None])[state]

        #command completition for more than one word
        if words[0] in self.gd_commands.keys():
            begin = readline.get_begidx()
            end = readline.get_endidx()
            being_completed = buffer[begin:end]
            logging.debug('.')
            logging.debug('origline=%s', repr(buffer))
            logging.debug('begin=%s', begin)
            logging.debug('end=%s', end)
            logging.debug('being_completed=%s', being_completed)
            logging.debug('words=%s', words)

            try:
                cnt=0
                base_candidates = self.gd_commands
                max_cnt = len(words) -1
                #if being_completed=='': max_cnt-=1
                while cnt<max_cnt:
                    first = words[cnt]
                    cnt +=1
                    base_candidates = base_candidates[first]

                candidates = base_candidates.keys()

                if being_completed:
                    # match options with portion of input
                    # being completed
                    self.current_candidates = [ w for w in candidates
                                                if w.startswith(being_completed) ]
                else:
                    # matching empty string so use all candidates
                    self.current_candidates = candidates

                logging.debug('candidates=%s', self.current_candidates)

            except (KeyError, IndexError), err:
                logging.error('completion error: %s', err)
                self.current_candidates = []

            try:
                response = self.current_candidates[state]
            except IndexError:
                response = None
            logging.debug('complete(%s, %s) => %s', repr(text), state, response)
            if len(self.current_candidates)>0:
                return response

        # # check if we should do path completion
        # cmd = line[0]
        # if not self.cmd_lookup:
        #     self._generate(cmd)
        # if not cmd in self.cmd_lookup or not self.cmd_lookup[cmd]:
        #     return None

        if state == 0:
            self.path_lookup = self._complete_path(os.path.expanduser(text))
        return (self.path_lookup + [None])[state]

    def complete_me(self, text, state):
        response = None
        if state == 0:
            # This is the first time for this text, so build a match list.

            origline = readline.get_line_buffer()
            begin = readline.get_begidx()
            end = readline.get_endidx()
            being_completed = origline[begin:end]
            words = origline.split()

            logging.debug('origline=%s', repr(origline))
            logging.debug('begin=%s', begin)
            logging.debug('end=%s', end)
            logging.debug('being_completed=%s', being_completed)
            logging.debug('words=%s', words)

            if not words:
                self.current_candidates = sorted(self.options.keys())
            else:
                try:
                    cnt=0
                    base_candidates = self.options
                    max_cnt = len(words)
                    if being_completed: max_cnt-=1

                    while cnt<max_cnt:
                        first = words[cnt]
                        cnt +=1
                        base_candidates = base_candidates[first]

                    candidates = base_candidates.keys()
                    logging.debug('candidates=%s', self.current_candidates)
                    if len(candidates)<1:
                        return self.complete_max(self, text, state)

                    if being_completed:
                        # match options with portion of input
                        # being completed
                        self.current_candidates = [ w for w in candidates
                                                    if w.startswith(being_completed) ]
                    else:
                        # matching empty string so use all candidates
                        self.current_candidates = candidates

                    logging.debug('candidates=%s', self.current_candidates)

                except (KeyError, IndexError), err:
                    logging.error('completion error: %s', err)
                    self.current_candidates = []

        try:
            response = self.current_candidates[state]
        except IndexError:
            response = None
        logging.debug('complete(%s, %s) => %s', repr(text), state, response)
        return response


def input_loop():
    line = ''
    while line != 'stop':
        line = raw_input('Prompt ("stop" to quit): ')
        print 'Dispatch %s' % line



if __name__ == "__main__":
    #just for testing
    ccdict = {'geounit':
                {'start':{'one':{}, 'two':{}, 'three':{}}, 'delete':{}},
              'stop':{}
              }
    print("special commands start with  --")
    comp = BufferAwareCompleter(ccdict,'--')
    # we want to treat '/' as part of a word, so override the delimiters
    readline.set_completer_delims(' \t\n')
    if 'libedit' in readline.__doc__:
        readline.parse_and_bind("bind ^I rl_complete")
    else:
        readline.parse_and_bind("tab: complete")
    readline.set_completer(comp.complete)

    # Prompt the user for text
    input_loop()

