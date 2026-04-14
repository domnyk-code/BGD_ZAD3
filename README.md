# Orkiestracja procesu ELT - zadanie
**Autor**: Dominik Nykiel, s24813 

## Uruchamianie kodu
W celu przetestowania kodu wystarczy użyć komendy:
```
docker compose up -d
``` 
aby postawić wszystkie kontenery Docker, a następnie zalogować się do UI Airflow na `localhost:8080`. 
Aby uzyskać hasło można użyć komendy
```
docker compose logs airflow | grep password
```
Należy pamiętać o tym aby utworzyć folder data w folderze głównym projektu i umieścić tam odpowiedni plik z danymi.
## Opis problemu
Niniejsze zadanie stanowi rozszerzenie projektu pipeline'u danych w oparciu o architekturę medalionową. Do zadania wykorzystane zostały zbiory danych odnośnie kursów taksówek w obrębie miasta Nowy Jork, pozyskane z oficjalnej strony *NYC Taxi & Limousine Commission*. Celem zadania jest stworzenie wstępnego pipeline'u przetwarzania tych danych, z wykorzystaniem orkiestracji procesów, w taki sposób aby cały proces wymagał jak najmniej fizycznej interakcji użytkownika.

## Opis danych
Link do danych: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page  

Każdy rekord w zbiorze danych opisuje jeden kurs taksówki zarejestrowany przez przewoźnika obsługującego dany pojazd. W każdym rekordzie występują następujące pola:  
- **VendorID** - Numeryczny identyfikator przewoźnika.  
- **tpep_pickup_datetime** - Data i godzina rozpoczęcia kursu.  
- **tpep_pickup_datetime** - Data i godzina zakończenia kursu.  
- **passenger_count** - Liczba pasażerów w kursie.  
- **trip_distance** - Długość kursu (w milach).  
- **RatecodeID** - Numeryczny identyfikator sposobu wyliczana opłaty za kurs.  
- **store_and_fwd** - Flaga, czy dane o kursie zostały od razu przesłane do przewoźnika, czy zachowane w pamięci pojazdu.  
- **PULocationID** - Numeryczny identyfikator miejsca rozpoczęcia kursu.  
- **DOLocationID** - Numeryczny identyfikator miejsca zakończenia kursu.  
- **payment_type** - Numeryczny identyfikator sposobu zapłaty za kurs.  
- **fare_amount** - Podstawowa opłata za kurs (w dolarach, centach).  
- **extra** - Jakiekolwiek dodatkowe opłaty (w dolarach, centach).  
- **mta_tax** - Kwota podatku wyliczona na podstawie używanej normy wyliczania opłaty za kurs.  
- **tip_amount** - Kwota napiwku. Liczone są tylko napiwki wykonane kartą płatniczą.  
- **tolls_amount** - Suma opłat za przejazd podczas kursu (ang. toll).  
- **total_amount** - Suma wszystkich opłat i napiwków.  
- **congestion_surcharge** - Kwota dodatkowej opłaty dla kursów w godzinach szczytu.  

Dane podzielone są na lata, a każdy rok jest podzielony na miesiące, czyli dla każdego roku mamy dwanaście zbiorów danych.  

## Zastosowane technologie
### Apache Airflow
![Airflow logo](/images/airflow_logo.png) 

Apache Airflow to narzędzie do wizualnego tworzenia, organizowania i monitorowania przepływów pracy oraz uruchamiania łańcuchów zadań. W tym projekcie służy jako główny silnik orkiestracyjny dla utworzonego pipeline'u.
### DBT
![DBT logo](/images/dbt_logo.png) 

DBT to narzędzie służące do transformacji danych załadowanych do bazy, wykorzystujące pliki sql. W tym projekcie jest użyte do stworzenia warstw silver i gold w architekturze medalionowej.
### Docker
![Docker logo](/images/docker_logo.png) 

Docker to platforma i oprogramowanie służące do konteneryzacji aplikacji, pozwalające uruchamianie programów w wirtualnych kontenerach. W tym projekcie Docker jest wykorzystany aby skonteneryzować Airflow i bazę danych PostgreSQL, w celu szybszego uruchomienia pipeline'u.
## Diagram przepływu danych
![Data pipeline diagram extended](/images/pipeline_diagram.png)
## Jak przetwarzano dane
Dane zostały pobrane ze strony NYC TLC, w formacie `parquet`. Pliki zawierające dane zostały umieszczone w folderze `data`, a następnie poddane operacjom z uwzględnieniem architektury medalionowej.
- W warstwie brązowej, zawierającej się w pliku DAG dla Airflow, suche dane zostają wczytane do bazy danych w tabeli z ogólnymi formatami kolumn. Dodane jest pole opisujące źródło danych (nazwa pliku) oraz moment czasowy załadowania danych (timestamp). Wykorzystana tutaj jest biblioteka **pandas** do załadowania danych do data frame'u.
- W warstwie srebrnej dane są przekopiowane z warstwy brązowej z konwersją na odpowiednie typy kolumn. Dokonana zostaje wstępna filtracja rekordów,  Wyszczególnione są również rekordy z informacjami nieprawidłowymi w świetle zasad przyjętych dla danych. Do przetwarzania danych w tej warstwie wykorzystane jest już **DBT**, w połączeniu z odpowiednimi plikami SQL.
- W warstwie złotej utworzone zostają tabele zawierające konkretne informacje, w oparciu o agregacje danych z tabeli srebrnej. Utworzone zostały tabele analizujące dzienne statystyki kursów, statystyki operatorów taksówek, i tabele zawierające podejrzane rekordy na podstawie różnych kryteriów. Tutaj również zastosowane są narzędzia **DBT** do wytworzenia tabel.

## Jak wygląda orkiestracja procesów
W pliku `nyc_taxi_pipeline.py` znajduje się główny DAG pipeline'u zawierający w sobie wszystkie skrypty jako zadania Airflow. Najpierw odpalany jest zadanie `ingest_bronze`, uruchamiające skrypt w Pythonie tworzący warstwę brązowo. Następnie odpalane są zadania DBT: `dbt_silver`, `dbt_gold` i `dbt_test`, tworzące odpowiednie warstwy i przeprowadzające testy. Te skrypty odpalane są za pomocą BashOperator, w celu odpowiedniego użycia DBT.
![Airflow pipeline diagram](/images/airflow_dag_scheme.png)
## Co zostało zmienione względem poprzedniego zadania?
- Uruchamiając PostgreSQL z Dockera wykorzystany jest `volume` pozwalając danym "przetrwać" zamknięcie kontenerów.
- Dane ani tabele nie są już usuwane przy każdym przejściu skryptów; warstwy bronze i silver tworzone są przyrostowo.
- Dodano testy DBT, pozwalające sprawdzić jakość danych wejściowych - aktualnie tylko na warstwie srebrnej.
## Co można ulepszyć w projekcie?
- Ulepszenie części orkiestracyjnej projektu Airflow poprzez użycie pełnej implementacji w Dockerze, zamiast okrojonej wersji `standalone`.
- Lepsze wykorzystanie dbt, dodanie nowych funkcji przetwarzania danych. Aktualnie jest to dość prosty schemat.
- Dodanie kontenera pgAdmin lub połączenie bazy danych z innym graficznym interfejsem aby można było łatwiej przeglądać bazę danych.
- Dodanie pobierania lub strumieniowania danych z API NYC, zamiast fizycznych plików.
  
Ogólnie mówiąc projekt stanowi akceptowalny wstęp do Airflow i dbt, ale może zostać ulepszony poprzez lepsze wykorzystanie funkcjonalności, które dodają te technologie.

# ERD dla poszczególnych warstw
## Warstwa brązowa
![ERD diagram for bronze layer of taxi trip database](/images//bronze_ERD.png)

## Warstwa srebrna
![ERD diagram for silver layer of taxi trip database](/images/silver_ERD_new.png)

## Warstwa złota
![ERD diagram for gold layer of taxi trip database](/images/gold_ERD.png)
