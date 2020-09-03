import cv2

class MyVideoCapture(cv2.VideoCapture):
    def __enter__(self):
        return self
    def __exit__(self, *args):
        self.release()