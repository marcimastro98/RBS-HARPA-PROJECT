# Documentazione dell'architettura del progetto

## Introduzione
In questa documentazione, verrà descritta l'architettura del progetto RBS-HARPA. L'obiettivo è fornire una panoramica generale della struttura del sistema e dei suoi componenti principali; un particolare focus verrà dato alle motivazioni per le quali il progetto è stato strutturato in questa maniera.

Nell'analisi dell'architettura particolare attenzione alla soddisfazione dei seguenti requisiti:

- utilizzo di un approccio *cloud ready* per una più veloce prototipazione del progetto e una più ampia flessibilità e scalabilità sull'infrastrttura di produzione: lo sviluppato attuato tramite *containerizzazione* ha garantito una modalità di approccio universale per tutti i membri de team, unificando l'esperienza di sviluppo e garantendo l'uniformità dei risultati ottenuti;

- versionamento del codice tramite *Git* per permettere sin dalle prime fasi di mantenere il codice sorgente pulito, garantire durante il progetto una migliore possibilità di collaborazione asincrona all'interno del *team* dislocato in maniera remota, dare la possibilità di eseguire *refactoring* del codice senza il rischio di perdere funzionalità;

- possibilità di poter automatizzare il flusso operativo, mantenendo quindi aperta la possibilità di rendere indipendenti i flussi di estrazione, trasformazione e caricamento del dato oltre che ulteriori sistemi di *data quality* che possono essere integrati;

- no *vendor lock-in*: per garantire la possibilità ad ogni membro del *team* di utilizzare sistemi operativi differenti (*Windows*, *MacOS*, *Linux*) e all'ambiente di produzione di poter essere efficientato nel miglior compromesso richiesto tra prestazioni, *know-how interno* e costi.

- realizzazione di una *data platform* modulare e che dia la possibilità di poter sviluppare ulteriormente nuovi requisiti.

## Architettura a livelli
Il progetto segue un'architettura a livelli, che consente una separazione chiara delle responsabilità all'interno dell'infrastrttura stessa e una migliore manutenibilità del codice. 

I livelli principali dell'architettura sono:

1. *Layer* estrazione del dato: HARPA si è impegnata nel fornire i dati attraverso 3 file *csv*; ulteriori dati sono stati estratti con integrazioni sviluppate in *Python* come ad esempio i dati meteoreologici sfruttanto *API* dedicate;  

2. *Layer* trasformazione e pulizia del dato: tramite *Python* i dati vengono puliti e normalizzati, risolte eventuali anomalie, gestite casistiche che hanno richiesto un'analisi di qualità del dato più approfondita;

3. *Layer* caricamento del dato: con *Python* i dati vengono infine caricati su un database *Postgresql*, nel quale vengono popolate differenti tabelle in base alla granularità dell'aggregazione: per favorire l'analisi e il test di differenti modelli soprattutto nelle prime fasi di analisi esplorativa sono state create le seguenti tabelle:

- 

## Componenti principali
I componenti principali del progetto RBS-HARPA sono:

1. Interfaccia utente: Questo componente gestisce l'interazione con l'utente e la presentazione dei dati.
2. Servizi di business: Questi componenti implementano la logica di business del sistema.
3. Servizi di accesso ai dati: Questi componenti si occupano dell'accesso ai dati e delle operazioni di persistenza.

## Diagramma dell'architettura
Di seguito è riportato un diagramma dell'architettura del progetto RBS-HARPA:

![Diagramma dell'architettura](/path/to/architecture_diagram.png)

## Conclusioni
Questa documentazione ha fornito una panoramica dell'architettura del progetto RBS-HARPA. Per ulteriori dettagli su ciascun componente e sulle interazioni tra di essi, consultare la documentazione tecnica specifica.
