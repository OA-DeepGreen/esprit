from esprit.raw import elasticsearch_url

from datetime import datetime, timedelta
import requests


class BadSnapshotMetaException(Exception):
    pass


class TodaySnapshotMissingException(Exception):
    pass


class FailedSnapshotException(Exception):
    pass


class SnapshotDeleteException(Exception):
    pass


class ESSnapshot(object):
    """ Representation of an ES Snapshot """
    def __init__(self, snapshot_json):
        self.data = snapshot_json
        self.name = snapshot_json['snapshot']
        self.state = snapshot_json['state']
        self.datetime = datetime.utcfromtimestamp(snapshot_json['start_time_in_millis'] / 1000)

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class ESSnapshotsClient(object):
    """ Client for performing operations on the ES Snapshots """

    def __init__(self, connection, snapshot_repository):
        """
        Initialise the Client with a connection to ES
        :param connection: a raw.Connection object to the ES instance
        :param snapshot_repository: the S3 repo identifier defined in the snapshot settings
        """
        self.snapshots = []
        # Replace the existing connection's index with the snapshot one
        connection.index = '_snapshot'
        self.snapshots_url = elasticsearch_url(connection, type=snapshot_repository)

    def list_snapshots(self):
        """
        Return a list of all snapshots in the S3 repository
        :return: list of ESSnapshot objects, oldest to newest
        """

        # If the client doesn't have the snapshots, ask ES for them
        if not self.snapshots:
            resp = requests.get(self.snapshots_url + '/_all', timeout=600)

            if 'snapshots' in resp.json():
                try:
                    snap_objs = [ESSnapshot(s) for s in resp.json()['snapshots']]
                except Exception as e:
                    raise BadSnapshotMetaException("Error creating snapshot object: " + e.message + ";")

                # Sort the snapshots old to new
                self.snapshots = sorted(snap_objs, key=lambda x: x.datetime)

        return self.snapshots

    def check_today_snapshot(self):
        """ Check we have a successful snapshot for today """
        snapshots = self.list_snapshots()
        if snapshots[-1].datetime.date() != datetime.utcnow().date():
            raise TodaySnapshotMissingException('Snapshot appears to be missing for {}'.format(datetime.utcnow().date()))
        elif snapshots[-1].state != 'SUCCESS':
            raise FailedSnapshotException('Snapshot for {} has failed'.format(datetime.utcnow().date()))

    def delete_snapshot(self, snapshot):
        """
        Delete a snapshot from S3 storage
        :param snapshot: An ESSnapshot object
        :return: The status code of the response to our delete request
        """
        resp = requests.delete(self.snapshots_url + '/' + snapshot.name, timeout=600)

        # Return success if we get a 2xx response
        return 200 <= resp.status_code < 300

    def prune_snapshots(self, ttl_days, delete_callback=None):
        """
        Delete all snapshots outwith our TTL (Time To Live) period based on today's date.
        :param ttl_days: integer number of days a snapshot should be retained
        :param delete_callback: callback to run after the delete has occurred, should accept an ESSnapshot and
        boolean success / fail: f(snapshot, succeeded)
        :return: nothing, but throws SnapshotDeleteException if not all were successful.
        """
        snapshots = self.list_snapshots()

        # Keep a list of boolean success / failures of our deletes
        results = []
        for snapshot in snapshots:
            if snapshot.datetime < datetime.utcnow() - timedelta(days=ttl_days):
                results.append(self.delete_snapshot(snapshot))
                if delete_callback:
                    delete_callback(snapshot, results[-1])

        # Our snapshots list is outdated, invalidate it
        self.snapshots = []

        print "snapshots prune results: {}".format(results)
        if not all(results):
            raise SnapshotDeleteException('Not all snapshots were deleted successfully.')
