import koolie.config.item


if __name__ == '__main__':

    load = koolie.config.item.Load()

    load.load('test_base.yaml')

    print(load.items())

    print(load.keys())

    base = next(iter(load.items()))
    print(type(base))
    print(base.type())
    print(base.tag())
    print(base.value())
