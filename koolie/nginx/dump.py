import datetime
import logging
import time
import uuid

import koolie.tools.common
import koolie.nginx.config

_logger = logging.getLogger(__name__)

METADATA_ID = 'id'
METADATA_STARTED = 'started'
METADATA_STOPPED = 'stopped'


class Dump(object):

    def __init__(self, config: koolie.nginx.config.Config) -> None:
        super().__init__()

        if config is None:
            self.__config = koolie.nginx.config.Config()
        else:
            self.__config = config

        self.__metadata = {}

    def config(self) -> koolie.nginx.config.Config:
        return self.__config

    def metadata(self) -> dict:
        return self.config().dump_metadata()

    def start(self):
        self.metadata()[METADATA_ID] = str(uuid.uuid4())
        self.metadata()[METADATA_STARTED] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    def stop(self):
        self.metadata()[METADATA_STOPPED] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    def source_id(self, source: dict) -> str:
        return '{}/{}/{}'.format(source[koolie.nginx.config.TYPE_KEY], source[koolie.nginx.config.NAME_KEY], source.get(koolie.nginx.config.TAG_KEY))

    def file_prefix(self) -> str:
        return \
            '# Koolie\n' \
            '# Load [{}]\n' \
            '\n' \
            .format(str(self.config().load_metadata()))

    def block_prefix(self, source: dict) -> str:
        return '# Source [{}]\n'.format(self.source_id(source))

    def dump(self):
        self.dump_nginx()
        self.dump_servers()
        self.dump_locations()

    def dump_config(self, file, items: list):
        for item in items:
            file.write(self.block_prefix(item))
            file.write(item[koolie.nginx.config.CONFIG_KEY])
            file.write('\n\n')

    def dump_nginx(self):

        def main(file, items: list):
            file.write('\n# main\n\n')
            self.dump_config(file, items)

        def events(file, items: list):
            file.write('\n# events\n\n')
            file.write('events {\n')
            self.dump_config(file, items)
            file.write('}\n')


        def http(file, items: list):
            file.write('\n# http\n\n')
            file.write('http {\n')
            self.dump_config(file, items)
            file.write('}\n')

        _logger.debug('dump_nginx()')
        try:
            file_name = '{}nginx.conf'.format(self.config().nginx_directory())
            with open(file=file_name, mode='w') as file:
                file.write(self.file_prefix())

                main(file, self.config().main()['main'])

                events(file, self.config().events()['events'])

                http(file, self.config().http()['http'])
        except Exception as exception:
            _logger.warning('dump_nginx() Exception [{}]'.format(koolie.tools.common.decode_exception(exception)))

    def dump_servers(self):
        _logger.debug('dump_servers()')
        try:
            for server in self.config().servers().keys():
                print(server)
                directory_name = '{}servers/'.format(self.config().nginx_directory())
                koolie.tools.common.ensure_directory(directory_name)
                file_name = '{}servers/{}.conf'.format(self.config().nginx_directory(), server)
                with open(file=file_name, mode='w') as file:
                    file.write(self.file_prefix())
                    file.write('server {\n')
                    self.dump_config(file, self.config().servers()[server])
                    file.write('\n\ninclude {}{}/*.conf;\n'.format(directory_name, server))
                    file.write('}')
        except Exception as exception:
            _logger.warning('dump_servers() Exception [{}]'.format(koolie.tools.common.decode_exception(exception)))

    def dump_locations(self):
        _logger.debug('dump_locations()')
        try:
            for location in self.config().locations().keys():
                print(location)
                print(self.config().locations()[location][0])

                directory_name = '{}servers/{}/'.format(self.config().nginx_directory(), self.config().locations()[location][0][koolie.nginx.config.SERVER_KEY])

                print('')
                # koolie.tools.common.ensure_directory(directory_name)
                # file_name = '{}{}.conf'.format(directory_name, location)
                # with open(file=file_name, mode='w') as file:
                #     file.write(self.file_prefix())
                #     file.write('server {\n')
                #     self.dump_config(file, self.config().servers()[location])
                #     file.write('\n\ninclude {}{}/*.conf;\n'.format(directory_name, location))
                #     file.write('}')
        except Exception as exception:
            _logger.warning('dump_servers() Exception [{}]'.format(koolie.tools.common.decode_exception(exception)))

    def __str__(self) -> str:
        return str(self.metadata())

