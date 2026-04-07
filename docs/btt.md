# 18. Bipartite Transfer Tiling (BTT)

La Fase 3 di RAPTOR tratta i foot-path come un insieme di rilassamenti locali del tipo $\tau_k(p') = \min\{\tau_k(p'), \tau_k(p) + l(p,p')\}$. Su un dataset moderno, pero', la rappresentazione a liste di adiacenza produce un pattern di accesso frammentato: si attraversano piccoli vettori sparsi fermata per fermata, con locality debole e continui salti di cache.

Alpha-RAPTOR sostituisce questa scansione puramente adiacente con **Bipartite Transfer Tiling (BTT)**, una riorganizzazione del grafo pedonale in blocchi sorgente-destinazione costruiti a load time a partire dagli stessi trasferimenti walker gia generati dal motore.

## 18.1 Hub Clustering Spaziale

Durante il bootstrap, le fermate vengono raggruppate in hub usando una quantizzazione spaziale per feed. La dimensione della cella non e' arbitraria: viene derivata dal raggio di camminata del motore e poi clampata nel range $[120, 220]$ metri, in modo da tenere piccoli i blocchi ma abbastanza stabili da collassare piattaforme e fermate di interscambio vicine nello stesso super-nodo.

Sia $\mathcal{H}_a$ un hub sorgente e $\mathcal{H}_b$ un hub destinazione. Tutti i foot-path che partono da fermate in $\mathcal{H}_a$ e terminano in fermate in $\mathcal{H}_b$ vengono raccolti in una tile densa:

$$
\mathbf{L}_{a,b} \in (\mathbb{N} \cup \infty)^{|\mathcal{H}_a| \times |\mathcal{H}_b|}
$$

Ogni cella contiene la durata del trasferimento migliore tra quella coppia di fermate, piu un riferimento all'arco walker originale che consente di ricostruire il parent step corretto in caso di miglioramento.

## 18.2 Rilassamento Tiled della Frontiera

A runtime, `relax_transfers_tiled(...)` non visita piu il frontier come una lista di stop indipendenti. Il motore:

1. fa una snapshot del transit frontier corrente
2. raggruppa gli stop sorgente per hub
3. per ogni tile $(\mathcal{H}_a, \mathcal{H}_b)$ valuta i candidati su memoria contigua
4. aggiorna i target usando ancora `record_stop_improvement(...)`

Per una tile fissata, il rilassamento e' un min-plus locale:

$$
\tau_k(\mathcal{H}_b) = \min\left(\tau_k(\mathcal{H}_b),\; \tau_k(\mathcal{H}_a) \otimes \mathbf{L}_{a,b}\right)
$$

Nel motore corrente l'operazione resta esplicita, ma l'ordine di memoria cambia radicalmente: i target della stessa tile sono contigui e i riferimenti agli archi originali sono gia materializzati nel blocco.

## 18.3 Conservazione della Semantica RAPTOR

BTT non cambia la semantica della Fase 3. In particolare:

- il frontier usato dal rilassamento resta una snapshot dei soli stop migliorati dal transit nel round corrente
- i nuovi stop migliorati via walk non vengono espansi ricorsivamente nello stesso round
- ogni miglioramento conserva `ParentStep::Walk` con `from_stop`, `duration_secs` e `distance_meters` coerenti con l'arco walker originale

Questa scelta mantiene l'equivalenza con il comportamento precedente, ma sostituisce la frammentazione della struttura `Vec<Vec<WalkTransfer>>` con un indice secondario ottimizzato per la scansione a blocchi.

## Note di Implementazione

- L'indice BTT viene costruito da `build_transfer_relax_index(...)` al load time dell'engine.
- La struttura originale dei trasferimenti resta disponibile per la ricostruzione delle walk polylines e per i path gia esistenti.
- Le tile sono ordinate deterministicamente per hub target e fermata target minima, per evitare derive non deterministiche nel tie-breaking.
- Se una fermata non ha coordinate, Alpha-RAPTOR la tratta come hub singolo, evitando cluster spurii.

## Impatto sul Motore

La novita' non e' l'introduzione di nuovi foot-path, ma il cambio di layout. BTT trasforma il passo piu frammentato della query in un rilassamento per blocchi sorgente-destinazione, migliorando locality e riducendo il costo di scansione dei trasferimenti densi senza rinunciare alla ricostruzione esatta del journey.