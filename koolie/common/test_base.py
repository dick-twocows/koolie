import koolie.common.base


if __name__ == '__main__':

    load = koolie.common.base.Load()

    load.load('test_base.yaml')

    print(load.items())

    print(load.items().keys())

    base = next(iter(load.items().values()))[0]
    print(type(base))
    print(base.type())
    print(base.tag())
    print(base.value())