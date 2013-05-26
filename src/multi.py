import util
import time

class multi_wrapper:
    """
    thread wraper for multiple jobqueus
    """

    def __init__(self,jqs):
        self._jqs = jqs
        self.err = util.err

    def mainloop(self):
        """
        run mainloop
        can be interupted by KeyboardInterrupt
        """
        for jq in self._jqs:
            jq.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            for jq in self._jqs:
                jq.stop()
