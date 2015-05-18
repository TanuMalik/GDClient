'''
http://pymotw.com/2/readline/
'''
import readline
import logging
import rlcompleter

LOG_FILENAME = '/tmp/completer.log'
logging.basicConfig(filename=LOG_FILENAME,
                    level=logging.DEBUG,
                    )

class BufferAwareCompleter(object):
    
    def __init__(self, options):
        self.options = options
        self.current_candidates = []
        return

    def complete(self, text, state):
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

    readline.set_completer(BufferAwareCompleter(ccdict).complete)

    # Use the tab key for completion
    readline.parse_and_bind('tab: complete')

    # Prompt the user for text
    input_loop()
