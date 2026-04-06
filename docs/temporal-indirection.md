# 15.5 Monotonia Sub-Lineare e Indici di Indirezione Temporale

L'assunto teorico alla base del Binary Search e del Quantized Time-Bucketing presuppone la monotonia stretta non decrescente dei tempi di partenza lungo le tuple di una rotta. L'applicazione empirica ai dataset GTFS mostra invece che questa monotonia viene spesso violata a livello di singola fermata da corse notturne, servizi circolari o ordinamenti anomali lato agenzia. Quando la searchability viene invalidata a livello globale di linea, il motore retrocede verso una scansione lineare nel hot path.

Alpha-RAPTOR isola quindi la monotonia al livello di fermata, e non piu al livello di linea, eliminando il degrado line-wide e confinando i dati sporchi al solo tratto topologico che li contiene.

## 15.5.1 Isolamento Per-Stop e Virtual Sorting

Sia $S_{dirty} \subseteq \mathcal{S}$ l'insieme delle fermate la cui sequenza di partenze viola l'ordinamento temporale del tensore principale. Per ogni $s \in S_{dirty}$, il builder offline costruisce un indirection array $\mathcal{I}_s$ che ordina virtualmente gli indici fisici delle corse secondo il tempo di partenza in quella fermata:

$$
\mathcal{I}_s = \operatorname{sort\_indices}(\mathcal{T}_s, \pi_{dep})
$$

L'array principale delle corse non viene riscritto. A runtime, la lookup temporale per una fermata non monotona usa $\mathcal{I}_s$ come ordine virtuale di ricerca. Chronos opera quindi su un asse temporale corretto anche quando i trip fisici rimangono memorizzati in un ordine incompatibile con la searchability diretta.

Sia $idx_{bucket}$ il bucket iniziale prodotto dal Quantized Time-Bucketing, la corsa di partenza viene recuperata sull'ordine virtuale:

$$
idx_{virtual} = \operatorname{Chronos}(\mathcal{I}_s, \tau)
$$

$$
t_{start} = \mathcal{T}_{main}[\mathcal{I}_s[idx_{virtual}]]
$$

Con questa stratificazione topologica, il caso peggiore per le fermate non monotone non degrada piu in una scansione lineare dell'intera linea. Il fallback non-monotonic viene annullato, e la telemetria residua separa i casi fisiologici di end-of-service dai casi patologici di dato sporco.

## 15.5.2 Propagazione del Pruning Scalare ai Profili Esatti

L'efficacia dell'involucro scalare sui profili spaziali rende necessaria la sua estensione ai bucket exact. Per ogni bucket exact $\mathcal{P}_{exact}(s_i, s_{dest})$, il motore mantiene un summary con:

- `absolute_min_duration_secs`
- `absolute_min_transfers`

Prima di scansionare la frontiera di Pareto interna al bucket, Alpha-RAPTOR applica un early exit costante:

$$
\tau_k(s_i) + \delta^*_{min}(\mathcal{P}_{exact}) \ge \tau^*_{arr}(s_{dest}) \implies \operatorname{Skip}(\mathcal{P}_{exact})
$$

Questo pruning impedisce la scansione di bucket exact interamente dominati dal bound globale. In pratica, l'intersezione tra pruning spaziale e pruning exact confina il motore ai soli rami che giacciono sotto il miglior arrivo noto, riducendo drasticamente il lavoro inutile nella fase di lookahead memoizzato.

## Note di Implementazione

- La searchability diretta viene ora mantenuta per fermata (`binary_searchable_by_stop`) anziche per linea.
- Le fermate non monotone usano un `trip_order_indirection_by_stop` serializzato nel core statico schema v6.
- I contatori runtime distinguono tra `chronos_bucket_fallback_end_of_service` e `chronos_bucket_fallback_non_monotonic`.
- I bucket exact applicano summary pruning prima della scansione dei profile point.

## Evidenza Empirica

Validazione live su `Conca d'Oro -> Laurentina @ 2026-04-06 09:05` dopo l'introduzione dello schema v6:

- `chronos_bucket_fallback_non_monotonic = 0`
- `chronos_bucket_fallback_searches = 0`
- `chronos_bucket_fallback_end_of_service` resta l'unico terminal miss fisiologico
- il warm path corretto resta sotto i 100 ms

Questa evoluzione elimina il fallback lineare imputabile alla non monotonia GTFS senza sacrificare la correttezza temporale del journey ricostruito.