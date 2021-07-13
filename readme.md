#### AirflowTrader

Arflow dags master. Набор дагов для работы традинг помощника. 
Отслеживания движения акций. Обнаружение и уведомление хороших событий для открытия (закрытия)
позиции.

#### Планы
 
 - Подготовка данных
   * SPBExchange API
   * OANDA API 
   
 - Алгоритмы
   * Обнаружение максимальных, минимальных значений по периодам 
   * MACD классическая 
   * MACD дивергенция 
   * Линии поддержки, треугольники
   * Линии фибоначчи 
  
 - Алгоритмы НС
   * на основе набора различных свечей (около 20 видов) рекурсивная нейронная сеть
  
  


#### Структура проекта

Проект написан для airflow, разбит на даги. 
Архитектура ETL pipline с разбиением на следующие этапы:
  - Extract - получение данных
  - Transform - подготовка данных (преобразование структуры, агрегирование, нормализация, смена СИ, вычищение, проверка)
  - Load - обработка данных, получение конечного результата


Точки входа:
  - [CommonInfoDag.py](TraderDailyDag.py) - даг сборки общих данных бирж


### Для чтение

- Терминалы для тайдинга:eSignal, Eikon, MetaStock, SMARTx (рос разработка), QUIK (популярный в россий), OnlineBroker,
  NASDAQ, NYSE, LSE, Hong Kong Stock Exchange и XETRA, Transaq Мобильные: Moex (Московская биржа), Finam Trade

- Для терминала QUIK есть библиотеки и решения:
  [github/QUIKSharp](https://github.com/finsight/QUIKSharp)
  [qlua](https://pypi.org/project/qlua/)
  [github/quik-lua-rpc](https://github.com/Enfernuz/quik-lua-rpc)

- Доступные API:
    - [к moex](https://www.moex.com/a2193)
    - [habr/moex](https://habr.com/ru/post/486716/)
    - [iss moex](https://iss.moex.com/iss/reference/)
    - [Спб биржа API](https://spbexchange.ru/ru/otc_market/repository/api/)
    - [tinkoff](https://tinkoffcreditsystems.github.io/invest-openapi/)
    - [moex ныло дальше api](http://ftp.micex.ru/pub/ClientsAPI)
    - [oandaapi](https://developer.oanda.com/rest-live-v20/instrument-ep/)
  
  

- Примеры
  [Пример на Moex API](https://habr.com/ru/post/343688/)


- Источники данных
  [tradingview](https://ru.tradingview.com/)

- Библиотеки
  [aimoex](https://wlm1ke.github.io/aiomoex)

- Брокеры OANDA БКС ВТБ


- Биржы:
    - [MOEX](https://www.moex.com/) - Московская. (Российские компании)
    - [spbexchange](https://spbexchange.ru/) Санкт-Петербурская
    - [NASDAQ](https://www.nasdaq.com) - американская биржа по технологическим компаниям. Приобретена Стокгольмская
      фондовая биржа OMX.
    - [NYSE](https://www.nyse.com) - Нью-Йорская фондовая биржа
    - [LSE]() - Лондонская фондовая биржа Подключена миланская биржа с образованием LSE.G.
    - [JASDAQ]() - Японская биржа
    - [SSE](http://www.sse.com.cn/) - Шанхайская фондовая биржа. (Основной индекс SSE Composite)
    - Euronext - Паньевропейская фондовая биржа. (Основной индекс Euronext 100. )
    - IPE - Лондонская международная нефтяная биржа
    - CCX - Чикаская климатическая биржа
    - NGX - биржа канадского газа
    - CHX - Чикагская фондовая биржа
    - [ICE](https://www.intercontinentalexchange.com) - оператор сети бирж. (Euronext, IPE, CCX, NYBOT, NGX, CHX)

## Основы

- [Numpy](https://pythonworld.ru/numpy/1.html)
- [Pandas](https://pythonru.com/tag/pandas)
- [Pipline pattern](https://khashtamov.com/ru/data-pipeline-luigi-python/)
- [Apache airflow](https://habr.com/ru/company/mailru/blog/339392/)

    

   