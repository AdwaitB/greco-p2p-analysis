from heapq import *


class Heap:
    def __init__(self, heap):
        self.heap = heap
        heapify(self.heap)

    def push(self, element):
        if element is not None:
            heappush(self.heap, element)

    def pop(self):
        return heappop(self.heap)

    def top(self):
        if self.empty():
            return None
        else:
            return self.heap[0]

    def empty(self):
        return len(self.heap) == 0

    def clear(self):
        self.heap.clear()
