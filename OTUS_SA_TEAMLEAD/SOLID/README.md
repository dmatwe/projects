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

Нарушенный принцип: Открытости/закрытости (Open/Closed Principle - OCP)

В этом блоке код AreaCalculator напрямую зависит от конкретных классов фигур (Rectangle и Circle). Если мы захотим добавить новую фигуру (например, Triangle), нам придется изменять метод calculate_area, что нарушает принцип OCP. Вместо этого, мы должны использовать полиморфизм, чтобы каждая фигура могла самостоятельно рассчитывать свою площадь.

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

Нарушенный принцип: Принцип подстановки Лисков (Liskov Substitution Principle - LSP)

В этом блоке класс Robot, который наследует от Worker, должен реализовать метод eat. Однако робот не ест, и его реализация метода eat является ненужной и не имеет смысла. Это нарушает LSP, поскольку объекты подкласса (Robot) не могут быть заменены объектами базового класса (Worker) без изменения поведения программы.


```json
# Блок No3
# Python
class Customer:
    def place_order(self, order):
        # ... код для размещения заказа self.send_notification (order)
    def send_notification(self, order):
        # ... код для отправки уведомления клиенту
```
Нарушенный принцип: Разделения интерфейса (Interface Segregation Principle - ISP) / S (Single Responsibility Principle, SRP) — Принцип единственной ответственности.

В этом блоке класс Customer отвечает как за размещение заказа, так и за отправку уведомлений. Это нарушает принцип ISP, который гласит, что клиенты не должны зависеть от интерфейсов, которые они не используют. Лучше разделить эти обязанности на два разных класса или интерфейса, чтобы каждый класс отвечал только за одну задачу.

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

Нарушенный принцип: Инверсии зависимостей (Dependency Inversion Principle - DIP)

В этом блоке класс Switch напрямую зависит от конкретного класса LightBulb. Это нарушает принцип DIP, который гласит, что высокоуровневые модули не должны зависеть от низкоуровневых модулей. Вместо этого оба должны зависеть от абстракций. Лучше было бы использовать интерфейс или абстрактный класс для лампочки, чтобы переключатель мог работать с любыми типами ламп.

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

Нарушенный принцип: Принцип подстановки Лисков (Liskov Substitution Principle - LSP)

В этом блоке класс Penguin наследует от Bird, но не может реализовать метод fly() корректно. Это приводит к тому, что при замене объекта Bird на Penguin поведение программы нарушается, так как пингвин не может летать. Вместо этого лучше создать отдельный класс для неплавающих птиц или использовать интерфейсы для разделения летающих и нелетающих птиц.