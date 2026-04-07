# 17. SIMD-Vectorized Route Traversal (SVRT)

Il collo di bottiglia classico della Fase 2 di RAPTOR non e' solo la ricerca della corsa utile, ma la ripetizione della stessa decisione di catch-up su ogni singola fermata della linea. Nel hot loop, il motore deve verificare se il bound del round precedente puo' ancora intercettare una corsa migliore rispetto a quella gia' attiva. Quando questa decisione viene eseguita fermata per fermata, il costo non e' piu soltanto algoritmico: diventa micro-architetturale, perche' il branch viene rivalutato milioni di volte su dati temporali irregolari.

Alpha-RAPTOR introduce quindi **SIMD-Vectorized Route Traversal (SVRT)** come fast path del route scan. L'obiettivo non e' vettorializzare tutta la Fase 2, ma collassare il test di catch-up su blocchi contigui di fermate e saltare interi chunk quando il predicato risulta vuoto.

## 17.1 Zero-Mask Trip Catch-Up

Sia $W=8$ la larghezza del blocco vettoriale usato nel motore corrente. Durante `scan_line`, quando esiste gia una corsa attiva $t$ e tale corsa non presenta delta realtime o fermate skipped, Alpha-RAPTOR materializza il vettore delle partenze schedulate del trip attivo sul blocco $[p_i, \dots, p_{i+W-1}]$:

$$
\mathbf{V}_{dep}(t) = [\tau_{dep}(t, p_i), \dots, \tau_{dep}(t, p_{i+W-1})]
$$

In parallelo viene letto, tramite gather, il bound del round precedente sulle stesse fermate:

$$
\mathbf{V}_{k-1} = [\tau_{k-1}(p_i), \dots, \tau_{k-1}(p_{i+W-1})]
$$

Il predicato di catch-up diventa allora una comparazione vettoriale stretta:

$$
\mathbf{M}_{catch} = \operatorname{SIMD\_CMP\_LT}(\mathbf{V}_{k-1}, \mathbf{V}_{dep}(t))
$$

Se $\mathbf{M}_{catch} = \mathbf{0}$, nessuna fermata del blocco puo' sostituire la corsa attiva con una partenza migliore. Il motore salta quindi tutte le lookup di `find_earliest_trip` per l'intero chunk e continua a propagare solo gli arrivi onboard. In questo caso il throughput del blocco diventa memory-bound, non branch-bound.

## 17.2 Condizione di Correttezza

Il fast path viene abilitato solo quando il trip attivo non ha modifiche realtime per-stop. Questa restrizione e' necessaria perche' il confronto vettoriale usa un buffer schedulato contiguo del trip, e quindi richiede che:

- `actual_departure == scheduled_departure` su tutto il blocco
- `is_stop_skipped == false` su tutto il blocco

Se la corsa attiva contiene delay o skip realtime, Alpha-RAPTOR retrocede automaticamente al percorso scalare gia presente. La correttezza del journey rimane invariata perche' il fast path non elimina stati raggiungibili: evita soltanto ricerche che il predicato vettoriale dimostra essere impossibili.

## Note di Implementazione

- Il motore usa `SVRT_WIDTH = 8`, coerente con AVX2 su interi a 32 bit.
- La routine `svrt_chunk_has_catchup_candidate(...)` usa AVX2 quando disponibile su `x86_64`, con fallback scalare identico sul piano semantico.
- Il path vettoriale vive dentro `scan_line`, non modifica Chronos e non cambia la logica di boarding/alighting.
- Gli arrivi onboard restano sempre processati, anche quando il catch-up del blocco viene saltato.

## Impatto sul Motore

SVRT non sostituisce l'algoritmo RAPTOR: ne comprime il costo nel punto in cui la frontiera e' gia salita a bordo e il problema dominante diventa capire se abbia senso rivalutare una corsa migliore. In pratica, Alpha-RAPTOR converte la parte piu ripetitiva del route scan in un test a maschera nulla, riducendo la densita' di branch nel tight loop senza introdurre approssimazioni.