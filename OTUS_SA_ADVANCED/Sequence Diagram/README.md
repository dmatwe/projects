[1. Чек-лист проведения интеграции](#s1)
<br/>
[2. Уровни абстракции](#s2)
<br/>
[3. Описание системных функций](#s3)
<br/>
[4. Описание логики взаимодействия сервисов и приложений](#s4)
<br/>
[5. Описание логики взаимодействия классов](#s5)
<br/>
[6. Элементы](#s6)
<br/>
[7. Линии жизни (Lifelines)](#s7)
<br/>
[8. Объекты диаграммы](#s8)
<br/>
[9. Сообщения (Messages)](#s9)
<br/>
[10. Создание и удаление](#s10)
<br/>
[11. Внешние сообщения](#s11)
<br/>
[12. Альтернативные потоки](#s12)
<br/>
[13. Опциональное выполнение](#s13)
<br/>
[14. Цикл](#s14)
<br/>
[15. Прочие Frame](#s15)
<br/>
[16. Диаграмма последовательности АТМ](#s16)
<br/>
[17. Робот и точка](#s17)


**Чек-лист проведения интеграции**
<a name="s1"></a>

1. Цели интеграции 
2. Уточнить возможные способы обмена 
3. Определить концептуальный набор данных для обмена 
4. Описать потоки данных 
5. Описать преобразования данных 

***Диаграмма нужна для наглядного примера, должна быть легко читаема, без лишних элементов***


![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq1.png)


**Уровни абстракции**
<a name="s2"></a>

•Описание системных функций
•Описание логики взаимодействия сервисов и приложений
•Описание логики взаимодействия классов

**Описание системных функций**
<a name="s3"></a>

1. На базе Use Case
2. Система как черный ящик
3. Описать основные доступные операции

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq2.png)

**Описание логики взаимодействия сервисов и приложений**
<a name="s4"></a>

1. На базе Use Case
2. Расписываются внутренние взаимодействия
3. Описываются логические методы

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq3.png)

**Описание логики взаимодействия классов**
<a name="s5"></a>

1. Описывают внутреннюю логику
2. Содержат методы классов
3. Не рисуются аналитиками

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq4.png)

**Элементы**
<a name="s6"></a>

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq5.png)


**Линии жизни (Lifelines)**
<a name="s7"></a>

1. Lifeline – линия жизни объекта в рамках последовательности
2. Activation Bar – плашка активации – период активности объекта в рамках времени жизни

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq6.png)

**Объекты диаграммы**
<a name="s8"></a>

1. Имя объекта – участника взаимодействия (модуль, интерфейс, сервис), если тип не принципиален
2. Актор (активный пользователь) – запустивший взаимодействие, если есть связь с Use Case
3. Ограничение или Интерфейс – граница системы, модуля или приложения, например, UI или API (форма запроса свободных столиков)
4. Контроллер – служебная сущность, управляющая прикладными сущностями, например, сервис проверки свободных столиков
5. Сущность – прикладная сущность, например, заказ или ресторан

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq7.png)

**Сообщения (Messages)**
<a name="s9"></a>

1. Синхронное сообщение – отправитель ожидает ответа от получателя
2. Асинхронное сообщение - отправитель не ожидает ответа от получателя и может выполнять свою последовательность дальше3. 
3. Ответное сообщение – ответ на синхронное сообщение 

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq8.png)

**Создание и удаление**
<a name="s10"></a>

1. Create – создание объекта в ходе последовательности
2. Destroy – уничтожение объекта из памяти


![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq9.png)

3. Self-message – вызов объектом самого себя: другого метода или рекурсивный вызов

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq10.png)


**Внешние сообщения** 
<a name="s11"></a>

1. Lost message – сообщение, отправленное адресату вне диаграммы, т.е. событие приема сообщения отсутствует или не имеет значения
2. Found message – сообщение, полученное от адресата вне диаграммы, т.е. событие отправки сообщения отсутствует или не имеет значения

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq11.png)

**Альтернативные потоки**
<a name="s12"></a>

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq12.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq121.png)


**Опциональное выполнение**
<a name="s13"></a>

***Opt Frame*** аналогичен Alt, однако в нем отсутствует блок else.

Таким образом, при выполнении условия – блок выполняется, при невыполнении – ничего не происходит

***Ref Frame*** – ссылка на связанную диаграмму последовательности

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq13.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq131.png)


**Цикл**
<a name="s14"></a>

***Loop Frame*** позволяет описать цикл обработки запросов, пока выполняется определенное условие.

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq14.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seq141.png)

**Прочие Frame (практически не используются в реальной жизни)**
<a name="s15"></a>

1. Break – работает аналогично opt, однако предназначено для обработки исключений (ошибок).
2. Parallel – описание параллельных потоков последовательности
3. Weak sequencing – сообщения внутри фрейма могут быть выполнены в произвольном порядке
4. Strict sequencing - сообщения внутри фрейма должны быть выполнены в строгом порядке
5. Negative – набор недопустимых сообщений
6. Critical – критически важный блок, допускает только один поток выполнения
7. Ignore – блок сообщений, который должен быть проигнорирован, например, сообщение в шине, полученное в этот момент не должно влиять на последовательность
8. Consider – обратно ignore. Любое сообщение не из этого списка игнорируется
9. Assertion – блок проверки на условия согласованности с другими диаграммами


**Диаграмма последовательности АТМ**
<a name="s16"></a>

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/seqbank.png)

**Диаграмма последовательности Робот и точка**
<a name="s17"></a>

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_ADVANCED/Sequence%20Diagram/png/sq.png)


```json
actor "Сотрудник производства" as A
box Упраление роботами #LightGreen   
participant  "Интерфейс управления роботами" as B 
participant  "Система управления роботами" as D
end box 
hide footbox
autonumber "<b>[0]"  
A -> B++:  Активировать робота 
B -> D++:  POST-запрос активации робота


ref over D
  Проверка состояния робота
  каждые 5 минут
end ref


opt Сбой робота
B <-- D:  Ошибка 404
Note over A: робот неисправен
A <-- B:  Уведомление о сбое
end

return Ответ 200: ОК
A <-- B:  Уведомление об успехе
B--


A -> B++:  Приготовить N бургеров
loop Max = N 
B -> D++:  POST-запрос на приготовление бургера
return Ответ 200: ОК
end
A <-- B:  Уведомление об успехе
B--
D--
......


A -> B++:  Назначить робота на точку грили 
B -> D++:  POST-запрос назначения робота
D -> D++:  Проверка свободна ли точка
D--



alt Точка занята
B <-- D:  Ошибка 409
Note over A: На точке уже есть робот
D--
A <-- B:  Уведомление об ошибке 
B--
A -> B++:  Заменить робота на точке
B -> D++:  POST-запрос замены робота
D -> D++:  Проверка точки
D--
B <-- D:  Ответ 200: ОК
A <-- B:  Уведомление об успехе
A--
B--



......
else Точка недоступна
D -> D++:  Проверка точки
D--
B <-- D:  Ошибка 404
Note over A: Оборудование вышло из строя
B++
A <-- B:  Уведомление об ошибке
A--
B--
......

else У робота низкий заряд
D -> D++:  Проверка состояния робота
D--
B <-- D:  Ошибка 409
Note over A: У робота низкий заряд
B++
A <-- B:  Уведомление об ошибке
B--
D--
A -> B++:  Отправить робота на зарядную станцию
B -> D++:  POST-запрос зарядки робота
D -> D++:  Проверка состояния робота
D--
B <-- D:  Ответ 200: ОК
A <-- B:  Уведомление об успехе
A--
B--
......


else Точка свободна
B <-- D:  Ответ 200: ОК
B++
A <-- B:  Уведомление об успехе
end
```
