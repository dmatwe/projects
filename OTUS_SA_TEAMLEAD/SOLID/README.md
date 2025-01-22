```json
# Блок No1
# Python
class AreaCalculator:
    def calculate_area(self, shape):
        if isinstance (shape, Rectangle): 
            return shape.width * shape.height 
        elif isinstance(shape, Circle):
            return math.pi * shape.radius ** 2 
        return 0
```

```json
# Блок No2
# Python

class Worker:
    def work(self):
        pass
    def eat(self):
        pass

class Robot (Worker):
    def work(self):
        # ... код для работы робота pass
    def eat(self):
        # ... робот не ест, это ненужная реализация pass
```

```json
# Блок No3
# Python
class Customer:
    def place_order(self, order):
        # ... код для размещения заказа self.send_notification (order)
    def send_notification(self, order):
        # ... код для отправки уведомления клиенту
```

```json
# Блок No4
# Python
class Switch:
    def __init__(self):
        self.bulb = LightBulb()
    def operate(self):
        # ... код для работы выключателя self.bulb.turn_on()

class Light Bulb:
    def turn_on(self):
        # ... код для включения лампочки pass

```

```json
# Блок No5
# Python
class Bird:
    def fly(self):
        # ... код для полета

class Penguin (Bird):
    def fly(self):
        raise NotImplementedError("Penguins can't fly.")
```