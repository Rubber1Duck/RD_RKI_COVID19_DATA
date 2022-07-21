# cronjob

Repo wurde initial geforkt von https://github.com/HrRodan/RKI_COVID19_DATA und anschließend auf meine Bedürfnisse verändert.

Quellenvermerk:

- Robert Koch-Institut (RKI), [dl-de/by-2-0](https://www.govdata.de/dl-de/by-2-0)

Täglich zwischen 1 und 6 Uhr GMT alle 15 Minuten schaut der cronjob nach neuen Daten vom RKI, sofern vorhanden werden sie geladen, aggregiert und die 7-Tage Neuinfektionen sowie die berechneten Inzidenzen in je eine Json Datei auf Landkreisebene und Bundeslandebene abgespeichert.
Ist der cronjob einmal erfolgreich durchgelaufen werden weitere cronjobs abgebrochen.
Die Bereitstellung der fixierten Inzidenzen erfolgt immer zuerst aus der offiziellen RKI Exceldatei. Fehlende Werte seit letzen Montag werden durch die Daten aus den beiden json Dateien ergänzt. Vorgehalten werden hier nur die letzen 10 Tage. (eventuell ist der Montag ein Feiertag, dann werden die Exceldaten erst Dienstags aktualiesiert, weitere bis zu drei Tage können dadurch abgefangen werden Datenumfang für 10 Tage ca. 800 kb)
