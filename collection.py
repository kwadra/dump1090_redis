from py1090 import FlightCollection

class FlightNotificationCollection(FlightCollection):
    def __contains__(self, item):
        """Check if an item is in the collection."""
        return item in self._dictionary

    def __delitem__(self, key):
        del self._dictionary[key]

    def add(self, message):
        """Adds a message to this collection.
        Args:
            message (:py:class:`Message`):
                message to add"""
        if hasattr(message, "notified") and message.notified:
            notified_time = message.notified
        else:
            notified_time = None

        super(FlightNotificationCollection, self).add(message)
        self._dictionary[message.hexident].notified = notified_time if notified_time else None