# Holographic Pedestrian Forest

Alpha-RAPTOR non usa piu' un servizio HTTP esterno per il primo e ultimo miglio delle query coordinate-coordinata. Il runtime usa una Holographic Pedestrian Forest (HPF) locale, costruita offline dal grafo pedonale OSM e caricata da cache binaria.

## Cosa fa

- Durante il build, il motore legge una sola volta il grafo pedonale OSM.
- Tutte le fermate GTFS ancorate al grafo vengono inserite come sorgenti in una multi-source Dijkstra limitata da `hpf.max_distance_meters`.
- Per ogni nodo OSM coperto, la cache HPF salva solo:
  - il codice Morton del nodo
  - il parent pointer verso la radice
  - la fermata GTFS radice piu' vicina
  - il costo pedonale cumulato
- A runtime, una coordinata viene convertita in Morton code, si cercano i nodi HPF vicini in ordine spaziale e si ricostruisce la polyline risalendo i parent pointer fino alla fermata.

Il grafo stradale non resta in RAM durante il runtime delle query. Restano solo la foresta compatta HPF e la matrice di trasferimenti stop-stop gia' usata dal motore.

## File generato

La cache HPF viene salvata sotto la runtime area nascosta del progetto come:

- `.alpha-raptor/cache/osm/lazio-latest.osm.hpf.bin`

La cache viene invalidata automaticamente se cambia il file OSM oppure se cambia l'impronta delle fermate GTFS.

## Configurazione

Nel manifest [alpha-raptor.toml](../alpha-raptor.toml) sono attivi due blocchi rilevanti:

```toml
[dvni]
knn_candidates = 5
max_walk_radius_meters = 1500.0

[hpf]
max_distance_meters = 4000.0
snap_tolerance_meters = 140.0
snap_quadratic_kappa_meters = 40.0
search_window = 512
```

Significato dei parametri:

- `dvni.knn_candidates`: numero massimo di connector pedonali candidati usati per l'iniezione dei nodi virtuali.
- `dvni.max_walk_radius_meters`: limite del fallback stop-level se HPF non e' disponibile.
- `hpf.max_distance_meters`: raggio massimo della multi-source expansion durante il build HPF.
- `hpf.snap_tolerance_meters`: soglia oltre la quale il connector viene classificato come asintotico nelle metriche di trace.
- `hpf.snap_quadratic_kappa_meters`: costante di smorzamento della penalita' quadratica di snap, usata nel costo `hpf_cost + snap + snap^2 / kappa`.
- `hpf.search_window`: ampiezza iniziale della finestra di ricerca sui Morton code HPF.

Le stesse impostazioni possono essere passate anche via environment:

- `ALPHA_DVNI_KNN`
- `ALPHA_DVNI_MAX_WALK_RADIUS_M`
- `ALPHA_HPF_MAX_DISTANCE_M`
- `ALPHA_HPF_SNAP_TOLERANCE_M`
- `ALPHA_HPF_SNAP_QUADRATIC_KAPPA_M`
- `ALPHA_HPF_SEARCH_WINDOW`

## API

L'endpoint resta [src/main.rs](../src/main.rs) su `/api/query`.

Per query coordinate-coordinata:

```text
/api/query?from_lat=41.94048&from_lon=12.52909&to_lat=41.82647&to_lon=12.48104&date=2026-04-06&time=09:05
```

Nel trace JSON sono esposti:

- `trace.coordinate_routing.connector_strategy`
- `trace.coordinate_routing.source_seed_count`
- `trace.coordinate_routing.destination_seed_count`
- `trace.coordinate_routing.source_asymptotic_connectors`
- `trace.coordinate_routing.destination_asymptotic_connectors`

Nelle metriche di build sono esposti anche:

- `build.hpf_strategy`
- `build.hpf_cache_hit`
- `build.hpf_covered_nodes`
- `build.hpf_anchored_stops`
- `build.timings.hpf_ms`

## Comportamento operativo

- Se HPF e' disponibile, DVNI usa i connector locali HPF e non effettua I/O esterno.
- Se HPF fallisce in build o non e' disponibile, il motore degrada a un fallback stop-level con distanza haversine diretta.
- Le polyline del primo e ultimo miglio vengono ricostruite localmente dai parent pointer HPF; non vengono scaricate da servizi terzi.

## Limiti attuali

- La fase di static reload differenziale non aggiorna HPF in-place: in caso di cambiamento fermate, HPF viene rigenerata o riletta da cache come blocco unico.
- Il lookup runtime usa una scansione crescente sullo spazio Morton. E' molto veloce in pratica, ma non equivale a una prova formale di nearest-neighbor ottimo su rete.
- Fuori copertura il sistema resta un'approssimazione controllata: il ranking privilegia l'aderenza al network con una penalita' quadratica di snap, ma il tratto finale fuori grafo resta euclideo.