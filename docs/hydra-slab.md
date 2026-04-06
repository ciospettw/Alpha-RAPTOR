# Hydra-Slab

Alpha-RAPTOR non usa piu' RocksDB per l'idratazione differita dei metadati statici. Il runtime usa Hydra-Slab, un backend puro Rust che separa i payload binari dai metadati d'indirizzamento e delega il paging al sistema operativo tramite `mmap`.

## Formato on-disk

Hydra-Slab scrive due file per ogni generazione del core statico:

- `*.hydra.data.bin`: slab sequenziale con i payload serializzati di stop, route, trip e shape points.
- `*.hydra.index.bin`: header tipizzato, tre array piatti di entry `(offset, len)` per stop/route/trip e una shape directory serializzata per le shape non ancora densificate numericamente.

Per stop, route e trip l'indice non usa piu' chiavi stringa o strutture KV. Il motore usa direttamente gli slot vettoriali interni gia' densi, quindi l'accesso all'entry dell'indice e' un semplice offset aritmetico nel file mmappato.

## Runtime

Il percorso di lettura e' il seguente:

1. Il server apre `data` e `index` con `memmap2`.
2. Per stop, route e trip legge l'entry a slot fisso dal rispettivo array nell'index slab.
3. Dall'entry recupera `offset` e `len` e taglia la slice corrispondente nel data slab.
4. La slice viene materializzata nel record Rust necessario alla risposta API.

Le shape non hanno ancora un dominio intero denso nel core statico, quindi Hydra-Slab mantiene per loro una shape directory Rust caricata una sola volta dall'index slab. Questo evita completamente RocksDB pur lasciando intatto il modello corrente delle trip shape.

## Overlay differenziale

Hydra-Slab include anche un overlay sparse in RAM per stop, route, trip e shape points. La risoluzione segue il pattern:

`overlay.get(id).unwrap_or_else(|| hydra_slab.get(id))`

Nel runtime corrente di Alpha-RAPTOR il reload statico continua a pubblicare una nuova generazione atomica del motore tramite `ArcSwap`, ma il layer overlay permette override puntuali senza modificare i file mmappati gia' aperti.

## Nota implementativa

- RocksDB e le dipendenze C/C++ sono state rimosse dal build.
- L'accesso a stop, route e trip e' `O(1)` con indice diretto.
- I payload restano mmap-backed, ma oggi vengono ancora deserializzati con `bincode` quando il motore costruisce i record di idratazione o le risposte JSON.

In termini di paper, Hydra-Slab realizza gia' la parte di mappatura diretta e di eliminazione del database generalista. Il passo successivo, se vorrai spingere davvero verso una idratazione completamente zero-copy, e' sostituire i record posseduti con viste borrowed sul data slab.

## Formula di accesso

Se `I_kind[x] = (omega_x, lambda_x)` e `D` e' il data slab mmappato, allora per stop/route/trip il recupero si riduce a:

`Omega(x) = D[omega_x .. omega_x + lambda_x]`

Nel repository attuale questo `x` coincide con lo slot vettoriale dell'entita' nel core statico.