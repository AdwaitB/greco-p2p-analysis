

class Link:
    def __init__(self, infra, dataset, link):
        self.infra = infra
        self.dataset = dataset
        self.link = link

        self.transfers = []
        self.free_at = 0

    def pop(self):
        self.transfers.pop(0)

    def push(self, data_id, now):
        """
        Updates the current free_at time, returns the pop element for the added new element
        :param data_id: The data_id
        :param now: The time of the job pop
        :return:
        """
        self.free_at = max(self.free_at, now)
        self.transfers.append(data_id)

        time = self.infra.get_time_for_p2p_transfer(self.link, self.dataset.get_size(data_id))
        self.free_at = self.free_at + time

        return (self.free_at, data_id, self.link[0], self.link[1], 'POP'), time

    def get_time(self, data_id, now):
        """
        Gets the time required for the transfer
        :param data_id: The data_id
        :param now: The time of the job pop
        :return: The time when the requested transfer will get over if scheduled
        """
        next_free_at = max(self.free_at, now)
        time = self.infra.get_time_for_p2p_transfer(self.link, self.dataset.get_size(data_id))
        return next_free_at + time
