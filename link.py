from random import randint


class Link:
    def __init__(self, infra, dataset, link):
        self.infra = infra
        self.dataset = dataset
        self.link = link

        self.transfers = {}             # <data_id, size remaining>
        self.incoming = {}              # <data_id, time when this entered in system>

        self.clock = 0                  # Time when the data was updated last

    def get_min(self):
        """
        Gets the entry for the first data_staging job that gets over.
        :return: Entry
        """
        if len(self.transfers) == 0:
            return None

        min_data_entry = list(self.transfers.keys())[0]
        min_data_time = self.infra.get_time_for_link_transfer(
            self.link, self.transfers[min_data_entry], len(self.transfers)
        )

        for data_id in self.transfers:
            time = self.infra.get_time_for_link_transfer(
                self.link, self.transfers[data_id], len(self.transfers)
            )

            if time < min_data_time:
                min_data_entry = data_id
                min_data_time = time

        # Second element is random to prevent colisions
        return min_data_time + self.clock, randint(0, 1000000000), self.link[0], \
                self.link[1], 'POP', min_data_entry, self.clock

    def update(self, time):
        """
        Updates the whole object to the time.
        :param time: time to update the system to
        :return: 0, if update failed (self.clock is more than this or get_min returned more than time) else 1
        """
        if len(self.transfers) == 0:
            self.clock = time
            return 0
        if self.get_min()[0] < time or self.clock > time:
            self.clock = time
            return 0

        diff = self.clock - time
        size_downloaded = self.infra.get_size_for_link_time(self.link, diff, len(self.transfers))

        for data_id in self.transfers:
            self.transfers[data_id] -= size_downloaded

        self.clock = time
        return 1

    def complete_transfer(self, job):
        """
        Completes the transfer for the given pop job
        :param job: the job as pushed inside the heap
        :return: a pop job for minimum time and the data_id, time_required to complete
        """
        # Stale entry, ignore it
        if job[6] < self.clock:
            return None, None
        else:
            data_id = job[5]
            now = job[0]

            total_time = now - self.incoming[data_id]

            # Update before deleting
            self.update(now)

            # Delete
            del self.transfers[data_id]
            del self.incoming[data_id]

            # print("Duration : {}, Quantity : {}, Src : {}, Dest : {}".format(
            #     total_time, self.dataset.get_size(job[5])/1000000000, job[2], job[3]
            # ))
            # print(str(total_time) + " " + str(job))

            # Return next min
            return self.get_min(), (data_id, total_time)

    def add_transfer(self, data_id, now):
        """
        Add the data_id to transfers.
        Return the event when the first transfer from the transfers is over
        :param data_id: The data_id
        :param now: The time of the job pop
        :return: None if the data_id is already transferred, else
        """
        if data_id in self.transfers:
            return None

        # Update before inserting
        self.update(now)

        self.transfers[data_id] = self.dataset.get_size(data_id)
        self.incoming[data_id] = now

        # print("ADD: time: {}, size: {}, min_end: {}, data_id: {}, expected_duration: {}, calculated_duration: {}, transfers: {}, src: {}, dest: {}, bw: {}".format(
        #     now, self.dataset.get_size(data_id)/1000000000, self.get_min()[0], data_id,
        #     self.dataset.get_size(data_id)/(self.infra.get_network_for_link(self.link)[0]*1000000),
        #     self.infra.get_time_for_link_transfer(self.link, self.dataset.get_size(data_id), len(self.transfers)),
        #     len(self.transfers),
        #     self.link[0], self.link[1], self.infra.get_network_for_link(self.link)[0]
        # ))
        return self.get_min()

    def get_bandwidth(self):
        """
        Get the bandwidth of this link given to a file if this file is scheduled
        :return: bw
        """
        return self.infra.get_network_for_link(self.link)[0]/(len(self.transfers) + 1)
