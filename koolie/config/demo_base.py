import koolie.config.items


if __name__ == '__main__':

    load = koolie.config.items.Load()

    load.load('single_item.yaml')

    print(load.items())

    print(load.keys())

    base = next(iter(load.items()))
    print(type(base))
    print(base.type())
    print(base.tag())
    print(base.value())
