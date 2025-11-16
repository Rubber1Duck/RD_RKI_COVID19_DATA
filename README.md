# RD_RKI_COVID19_DATA

Repo wurde initial geforkt von https://github.com/HrRodan/RKI_COVID19_DATA und anschließend auf meine Bedürfnisse verändert.

Quellenvermerk:

- Robert Koch-Institut (RKI), [dl-de/by-2-0](https://www.govdata.de/dl-de/by-2-0)

Täglich ab 1 Uhr GMT wird alle 15 Minuten geprüft ob ein neues Dump beim RKI vorliegt, sofern vorhanden werden es geladen, aggregiert und die "new", "accumulated", "history" sowie die "frozen-incidence" Werte im Ordner dataStore gespeichert.
Die new, accumulated und history Daten sind von diesem Tag. Die Frozen-incidence Werte werden seit dem vergangenen Montag vorgehalten (frühere Daten sind bei RKI abrufbar, jedoch wird die Datei mit den frozen-incidence Werten nur Montags aktualisiert

Ist der cronjob einmal erfolgreich durchgelaufen started der nächste cronjob erst wieder um 1 Uhr GMT des Folgetages.
