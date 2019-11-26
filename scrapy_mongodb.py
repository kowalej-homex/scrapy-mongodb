import datetime
import logging

import six
from sys import getsizeof
from pymongo import errors
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.read_preferences import ReadPreference
import gridfs
from scrapy.exporters import BaseItemExporter


def not_set(string):
    """Check if a string is None or ''.

    :returns: bool - True if the string is empty
    """
    if string is None:
        return True
    elif string == '':
        return True
    return False


class MongoDBPipeline(BaseItemExporter):
    """MongoDB pipeline."""

    # Default options
    config = {
        'uri': 'mongodb://localhost:27017',
        'fsync': False,
        'write_concern': 0,
        'database': 'scrapy-mongodb',
        'collection': 'items',
        'separate_collections': False,
        'replica_set': None,
        'unique_key': None,
        'buffer': None,
        'append_timestamp': False,
        'stop_on_duplicate': 0,
        'grid_fs_threshold_bytes': None,
        'grid_fs_field_tag': 'big_field',
    }

    # Item buffer
    current_item = 0
    item_buffer = []

    # Duplicate key occurence count
    duplicate_key_count = 0

    def __init__(self, **kwargs):
        """Constructor."""
        super(MongoDBPipeline, self).__init__(**kwargs)
        self.logger = logging.getLogger('scrapy-mongodb-pipeline')

    def load_spider(self, spider):
        self.crawler = spider.crawler
        self.settings = spider.settings

        # Versions prior to 0.25
        if not hasattr(spider, 'update_settings') and hasattr(spider, 'custom_settings'):
            self.settings.setdict(spider.custom_settings or {}, priority='project')

    def open_spider(self, spider):
        self.load_spider(spider)

        # Configure the connection
        self.configure()

        if self.config['replica_set'] is not None:
            connection = MongoReplicaSetClient(
                self.config['uri'],
                replicaSet=self.config['replica_set'],
                w=self.config['write_concern'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY_PREFERRED)
        else:
            # Connecting to a stand alone MongoDB
            connection = MongoClient(
                self.config['uri'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY)

        # Set up the database
        self.database = connection[self.config['database']]
        self.collections = {'default': self.database[self.config['collection']]}
        self.grid_fs = gridfs.GridFS(self.database)

        self.logger.info(u'Connected to MongoDB {0}, using "{1}"'.format(
            self.config['uri'],
            self.config['database']))

        # Get the duplicate on key option
        if self.config['stop_on_duplicate']:
            tmpValue = self.config['stop_on_duplicate']

            if tmpValue < 0:
                msg = (
                    u'Negative values are not allowed for'
                    u' MONGODB_STOP_ON_DUPLICATE option.'
                )

                self.logger.error(msg)
                raise SyntaxError(msg)

            self.stop_on_duplicate = self.config['stop_on_duplicate']

        else:
            self.stop_on_duplicate = 0

    def configure(self):
        """Configure the MongoDB connection."""
        # Handle deprecated configuration
        if not not_set(self.settings['MONGODB_HOST']):
            self.logger.warning(
                u'DeprecationWarning: MONGODB_HOST is deprecated')
            mongodb_host = self.settings['MONGODB_HOST']

            if not not_set(self.settings['MONGODB_PORT']):
                self.logger.warning(
                    u'DeprecationWarning: MONGODB_PORT is deprecated')
                self.config['uri'] = 'mongodb://{0}:{1:i}'.format(
                    mongodb_host,
                    self.settings['MONGODB_PORT'])
            else:
                self.config['uri'] = 'mongodb://{0}:27017'.format(mongodb_host)

        if not not_set(self.settings['MONGODB_REPLICA_SET']):
            if not not_set(self.settings['MONGODB_REPLICA_SET_HOSTS']):
                self.logger.warning(
                    (
                        u'DeprecationWarning: '
                        u'MONGODB_REPLICA_SET_HOSTS is deprecated'
                    ))
                self.config['uri'] = 'mongodb://{0}'.format(
                    self.settings['MONGODB_REPLICA_SET_HOSTS'])

        # Set all regular options
        options = [
            ('uri', 'MONGODB_URI'),
            ('fsync', 'MONGODB_FSYNC'),
            ('write_concern', 'MONGODB_REPLICA_SET_W'),
            ('database', 'MONGODB_DATABASE'),
            ('collection', 'MONGODB_COLLECTION'),
            ('separate_collections', 'MONGODB_SEPARATE_COLLECTIONS'),
            ('replica_set', 'MONGODB_REPLICA_SET'),
            ('unique_key', 'MONGODB_UNIQUE_KEY'),
            ('buffer', 'MONGODB_BUFFER_DATA'),
            ('append_timestamp', 'MONGODB_ADD_TIMESTAMP'),
            ('stop_on_duplicate', 'MONGODB_STOP_ON_DUPLICATE'),
            ('grid_fs_threshold_bytes', 'MONGODB_GRID_FS_THRESHOLD_BYTES'),
            ('grid_fs_field_tag', 'MONGODB_GRID_FS_FIELD_TAG')
        ]

        for key, setting in options:
            if not not_set(self.settings[setting]):
                self.config[key] = self.settings[setting]

        # Check for illegal configuration
        if self.config['buffer'] and self.config['unique_key']:
            msg = (
                u'IllegalConfig: Settings both MONGODB_BUFFER_DATA '
                u'and MONGODB_UNIQUE_KEY is not supported'
            )
            self.logger.error(msg)
            raise SyntaxError(msg)

    def process_item(self, item, spider):
        """Process the item and add it to MongoDB.

        :type item: Item object
        :param item: The item to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: Item object
        """

        # Filter out fields which are explictely marked MONGODB_GRID_FS_FIELD_TAG (default = 'big_field').
        grid_fields = list(
            dict(
                filter(lambda x: x[1].get(self.config['grid_fs_field_tag'], False) is True, item.fields.items())
            ).keys()
        )

        item = dict(self._get_serialized_fields(item))

        # If MONGODB_GRID_FS_THRESHOLD_BYTES is set: 
        # Find the values where the item's measured size is greater than the max size (in bytes).
        if self.config['grid_fs_threshold_bytes'] is not None:
            max_size_bytes = self.config['grid_fs_threshold_bytes']
            oversized = list(
                dict(
                    filter(lambda x: getsizeof(x[1]) > max_size_bytes, item.items())
                ).keys()
            )
            grid_fields += oversized

        item = dict((k, v) for k, v in six.iteritems(item) if v is not None and v != "")

        if self.config['buffer']:
            self.current_item += 1
            self.item_buffer.append((item, grid_fields))
            if self.current_item == self.config['buffer']:
                self.current_item = 0
                try:
                    return self.insert_item(self.item_buffer, spider)
                finally:
                    self.item_buffer = []
            return item

        return self.insert_item((item, grid_fields), spider)

    def close_spider(self, spider):
        """Be called when the spider is closed.

        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: None
        """
        if self.item_buffer:
            self.insert_item(self.item_buffer, spider)

    def insert_item(self, item_container, spider):
        """Process the item and add it to MongoDB.

        :type item: Tuple((Item object) or [(Item object)], [str])
        :param item: The item(s) to put into MongoDB and the grid_fs field keys.
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: Item object
        """

        if not isinstance(item_container, list):
            item = item_container[0]
            grid_fields = item_container[1]
            for key in item:
                if key in grid_fields:
                    file_key = self.grid_fs.put(str(item[key]), encoding='utf-8')
                    item[key] = file_key
            if self.config['append_timestamp']:
                item['scrapy-mongodb'] = {'ts': datetime.datetime.utcnow()}
        else:
            item = []
            for ic in item_container:
                new_item = ic[0]
                grid_fields = ic[1]
                for key in new_item:
                    if key in grid_fields:
                        file_key = self.grid_fs.put(str(new_item[key]), encoding='utf-8')
                        new_item[key] = file_key
                if self.config['append_timestamp']:
                    new_item['scrapy-mongodb'] = {'ts': datetime.datetime.utcnow()}
                item.append(new_item)

        collection_name, collection = self.get_collection(spider.name)

        if self.config['unique_key'] is None:
            try:
                collection.insert(item, continue_on_error=True)
                self.logger.debug(u'Stored item(s) in MongoDB {0}/{1}'.format(
                    self.config['database'], collection_name))

            except errors.DuplicateKeyError:
                self.logger.debug(u'Duplicate key found')
                if (self.stop_on_duplicate > 0):
                    self.duplicate_key_count += 1
                    if (self.duplicate_key_count >= self.stop_on_duplicate):
                        self.crawler.engine.close_spider(
                            spider,
                            'Number of duplicate key insertion exceeded'
                        )

        else:
            key = {}

            if isinstance(self.config['unique_key'], list):
                for k in dict(self.config['unique_key']).keys():
                    key[k] = item[k]
            else:
                key[self.config['unique_key']] = item[self.config['unique_key']]

            collection.update(key, item, upsert=True)

            self.logger.debug(u'Stored item(s) in MongoDB {0}/{1}'.format(
                self.config['database'], collection_name))

        return item

    def get_collection(self, name):
        if self.config['separate_collections']:
            collection = self.collections.get(name)
            collection_name = name

            if not collection:
                collection = self.database[name]
                self.collections[name] = collection
        else:
            collection = self.collections.get('default')
            collection_name = self.config['collection']

        # Ensure unique index
        if self.config['unique_key']:
            collection.ensure_index(self.config['unique_key'], unique=True)
            self.logger.info(u'Ensuring index for key {0}'.format(
                self.config['unique_key']))
        return (collection_name, collection)
