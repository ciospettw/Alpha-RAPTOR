const state = {
  stats: null,
  realtime: null,
  query: null,
  busyCount: 0,
  map: null,
  routeLayer: null,
  vehicleLayer: null,
  endpointLayer: null,
  mapSelectionTarget: "from",
  selectedEndpoints: {
    from: null,
    to: null,
  },
};

const dom = {
  fromSearch: document.getElementById("fromSearch"),
  fromStopId: document.getElementById("fromStopId"),
  fromResults: document.getElementById("fromResults"),
  toSearch: document.getElementById("toSearch"),
  toStopId: document.getElementById("toStopId"),
  toResults: document.getElementById("toResults"),
  routeModeSelect: document.getElementById("routeModeSelect"),
  dateInput: document.getElementById("dateInput"),
  timeInput: document.getElementById("timeInput"),
  maxTransfersInput: document.getElementById("maxTransfersInput"),
  queryForm: document.getElementById("queryForm"),
  swapButton: document.getElementById("swapButton"),
  pickFromButton: document.getElementById("pickFromButton"),
  pickToButton: document.getElementById("pickToButton"),
  mapHint: document.getElementById("mapHint"),
  statsGrid: document.getElementById("statsGrid"),
  itinerarySummary: document.getElementById("itinerarySummary"),
  legsContainer: document.getElementById("legsContainer"),
  traceContainer: document.getElementById("traceContainer"),
  realtimeContainer: document.getElementById("realtimeContainer"),
  refreshRealtimeButton: document.getElementById("refreshRealtimeButton"),
  refreshStatus: document.getElementById("refreshStatus"),
  topbarProgress: document.getElementById("topbarProgress"),
};

initialize();

async function initialize() {
  initializeDefaults();
  initializeMap();
  wireSearch("from", dom.fromSearch, dom.fromStopId, dom.fromResults);
  wireSearch("to", dom.toSearch, dom.toStopId, dom.toResults);
  dom.queryForm.addEventListener("submit", onSubmitQuery);
  dom.routeModeSelect.addEventListener("change", onRouteModeChange);
  dom.swapButton.addEventListener("click", swapStops);
  dom.pickFromButton.addEventListener("click", () => setMapSelectionTarget("from"));
  dom.pickToButton.addEventListener("click", () => setMapSelectionTarget("to"));
  dom.refreshRealtimeButton.addEventListener("click", refreshRealtime);
  syncRouteModeUI();
  setMapSelectionTarget("from");

  await Promise.all([loadStats(), loadRealtime()]);
}

function initializeDefaults() {
  const now = new Date();
  dom.dateInput.value = now.toISOString().slice(0, 10);
  dom.timeInput.value = `${String(now.getHours()).padStart(2, "0")}:${String(now.getMinutes()).padStart(2, "0")}`;
}

function initializeMap() {
  state.map = L.map("map", {
    zoomControl: true,
    scrollWheelZoom: true,
    doubleClickZoom: false,
  }).setView([41.9028, 12.4964], 12);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors",
    maxZoom: 19,
  }).addTo(state.map);

  state.routeLayer = L.layerGroup().addTo(state.map);
  state.vehicleLayer = L.layerGroup().addTo(state.map);
  state.endpointLayer = L.layerGroup().addTo(state.map);
  state.map.on("dblclick", onMapDoubleClick);
  setTimeout(() => state.map.invalidateSize(), 0);
}

function onMapDoubleClick(event) {
  setEndpointFromCoordinate(state.mapSelectionTarget, event.latlng.lat, event.latlng.lng, true);
  if (state.mapSelectionTarget === "from") {
    setMapSelectionTarget("to");
  }
}

function setMapSelectionTarget(prefix) {
  state.mapSelectionTarget = prefix;
  dom.mapHint.textContent = `Doppio click mappa: prossimo punto = ${prefix === "from" ? "origine" : "destinazione"}`;
  dom.pickFromButton.className = `mini-button ${prefix === "from" ? "is-active" : ""}`;
  dom.pickToButton.className = `mini-button ${prefix === "to" ? "is-active" : ""}`;
}

async function loadStats() {
  const releaseBusy = beginBusy();
  try {
    const payload = await fetchJson("/api/stats");
    state.stats = payload;
    renderStats();
  } finally {
    releaseBusy();
  }
}

async function loadRealtime() {
  const releaseBusy = beginBusy();
  try {
    const payload = await fetchJson("/api/realtime?limit=24");
    state.realtime = payload;
    renderRealtime();
  } finally {
    releaseBusy();
  }
}

async function refreshRealtime() {
  setRefreshStatus("sync", true);
  const releaseBusy = beginBusy();
  try {
    const payload = await fetchJson("/api/realtime/refresh", { method: "POST" });
    state.realtime = payload;
    renderRealtime();
    if (state.stats) {
      state.stats.realtime = payload;
      renderStats();
    }
    setRefreshStatus("ok", false);
  } catch (error) {
    setRefreshStatus("errore", false);
    dom.realtimeContainer.innerHTML = `<div class="error-box">${escapeHtml(error.message)}</div>`;
  } finally {
    releaseBusy();
  }
}

async function onSubmitQuery(event) {
  event.preventDefault();
  const routeMode = getRouteMode();
  const search = new URLSearchParams();

  try {
    if (routeMode === "drive-only") {
      search.set("mode", "drive");
      appendStreetEndpointParams(search, "from", dom.fromSearch);
      appendStreetEndpointParams(search, "to", dom.toSearch);
    } else {
      search.set("date", dom.dateInput.value);
      search.set("time", dom.timeInput.value);
      search.set("max_transfers", dom.maxTransfersInput.value);
      appendEndpointParams(search, "from", dom.fromSearch, dom.fromStopId);
      appendEndpointParams(search, "to", dom.toSearch, dom.toStopId);
    }
  } catch (error) {
    dom.itinerarySummary.textContent = error.message;
    dom.itinerarySummary.className = "summary-strip error-strip";
    return;
  }

  dom.itinerarySummary.textContent = routeMode === "drive-only"
    ? "Routing stradale in esecuzione..."
    : "Query in esecuzione...";
  dom.itinerarySummary.className = "summary-strip";

  const releaseBusy = beginBusy();
  try {
    const endpoint = routeMode === "drive-only" ? "/api/street" : "/api/query";
    const payload = await fetchJson(`${endpoint}?${search.toString()}`);
    state.query = payload;
    renderQuery();
  } catch (error) {
    state.query = null;
    dom.itinerarySummary.textContent = error.message;
    dom.itinerarySummary.className = "summary-strip error-strip";
    dom.legsContainer.innerHTML = '<div class="empty-state">Nessun itinerario.</div>';
    dom.traceContainer.innerHTML = `<div class="error-box">${escapeHtml(error.message)}</div>`;
    clearMapLayers();
    renderEndpointSelections();
  } finally {
    releaseBusy();
  }
}

function onRouteModeChange() {
  syncRouteModeUI();
  if (!state.query) {
    return;
  }

  markRouteStale(
    getRouteMode() === "drive-only"
      ? "Modalità drive only attiva. Premi Route."
      : "Modalità transit attiva. Premi Route.",
  );
}

function swapStops() {
  const fromLabel = dom.fromSearch.value;
  const fromId = dom.fromStopId.value;
  const fromSelection = state.selectedEndpoints.from;

  dom.fromSearch.value = dom.toSearch.value;
  dom.fromStopId.value = dom.toStopId.value;
  dom.toSearch.value = fromLabel;
  dom.toStopId.value = fromId;
  state.selectedEndpoints.from = state.selectedEndpoints.to;
  state.selectedEndpoints.to = fromSelection;
  renderEndpointSelections();
  markRouteStale("Punti invertiti. Premi Route.");
}

function wireSearch(prefix, input, hiddenInput, resultsContainer) {
  let timer = null;

  input.addEventListener("input", () => {
    hiddenInput.value = "";
    clearTimeout(timer);
    const value = input.value.trim();

    if (!value) {
      clearEndpoint(prefix, true);
      resultsContainer.innerHTML = "";
      return;
    }

    const coordinates = parseCoordinateInput(value);
    if (coordinates) {
      setEndpointFromCoordinate(prefix, coordinates.lat, coordinates.lon, false, true);
      resultsContainer.innerHTML = '<div class="search-result muted">Coordinate rilevate.</div>';
      return;
    }

    clearEndpoint(prefix, true);

    if (value.length < 2) {
      resultsContainer.innerHTML = "";
      return;
    }

    timer = setTimeout(async () => {
      const releaseBusy = beginBusy();
      try {
        const payload = await fetchJson(`/api/stops?q=${encodeURIComponent(value)}&limit=8`);
        renderSearchResults(prefix, payload, input, hiddenInput, resultsContainer);
      } catch (error) {
        resultsContainer.innerHTML = `<div class="search-result muted">${escapeHtml(error.message)}</div>`;
      } finally {
        releaseBusy();
      }
    }, 180);
  });

  input.addEventListener("blur", () => {
    setTimeout(() => {
      resultsContainer.innerHTML = "";
    }, 120);
  });
}

function setEndpointFromCoordinate(prefix, latitude, longitude, fromMap, markDirty = true) {
  const input = prefix === "from" ? dom.fromSearch : dom.toSearch;
  const hiddenInput = prefix === "from" ? dom.fromStopId : dom.toStopId;
  input.value = formatCoordinateValue(latitude, longitude);
  hiddenInput.value = "";
  state.selectedEndpoints[prefix] = {
    kind: "coordinate",
    name: `${prefix === "from" ? "Origine" : "Destinazione"} mappa`,
    lat: latitude,
    lon: longitude,
  };
  renderEndpointSelections();
  if (fromMap && state.query) {
    markRouteStale(`Nuova ${prefix === "from" ? "origine" : "destinazione"} da mappa. Premi Route.`);
  } else if (markDirty && state.query) {
    markRouteStale("Parametri aggiornati. Premi Route.");
  }
}

function setEndpointFromStop(prefix, stop) {
  const input = prefix === "from" ? dom.fromSearch : dom.toSearch;
  const hiddenInput = prefix === "from" ? dom.fromStopId : dom.toStopId;
  input.value = stop.name;
  hiddenInput.value = stop.id;
  state.selectedEndpoints[prefix] = {
    kind: "stop",
    id: stop.id,
    name: stop.name,
    lat: typeof stop.latitude === "number" ? stop.latitude : null,
    lon: typeof stop.longitude === "number" ? stop.longitude : null,
  };
  renderEndpointSelections();
  if (state.query) {
    markRouteStale("Parametri aggiornati. Premi Route.");
  }
}

function clearEndpoint(prefix, markDirty = true) {
  state.selectedEndpoints[prefix] = null;
  renderEndpointSelections();
  if (markDirty && state.query) {
    markRouteStale("Parametri aggiornati. Premi Route.");
  }
}

function renderEndpointSelections() {
  state.endpointLayer.clearLayers();
  const bounds = [];

  for (const prefix of ["from", "to"]) {
    const endpoint = state.selectedEndpoints[prefix];
    if (!endpoint || typeof endpoint.lat !== "number" || typeof endpoint.lon !== "number") {
      continue;
    }

    const latlng = [endpoint.lat, endpoint.lon];
    bounds.push(latlng);
    L.circleMarker(latlng, {
      radius: 8,
      color: prefix === "from" ? "#225ea8" : "#c0392b",
      fillColor: "#ffffff",
      fillOpacity: 1,
      weight: 3,
    })
      .bindTooltip(prefix === "from" ? "DA" : "A", {
        permanent: true,
        direction: "top",
        className: "endpoint-tooltip",
      })
      .bindPopup(`<strong>${escapeHtml(endpoint.name)}</strong><br/>${escapeHtml(formatCoordinateValue(endpoint.lat, endpoint.lon))}`)
      .addTo(state.endpointLayer);
  }

  if (state.query || !bounds.length) {
    return;
  }

  if (bounds.length === 1) {
    state.map.setView(bounds[0], 14);
  } else {
    state.map.fitBounds(bounds, { padding: [30, 30] });
  }
}

function appendEndpointParams(search, prefix, input, hiddenInput) {
  if (hiddenInput.value) {
    search.set(prefix, hiddenInput.value);
    return;
  }

  const coordinates = parseCoordinateInput(input.value);
  if (coordinates) {
    search.set(`${prefix}_lat`, String(coordinates.lat));
    search.set(`${prefix}_lon`, String(coordinates.lon));
    return;
  }

  throw new Error(`Specifica ${prefix === "from" ? "origine" : "destinazione"} come fermata o come coordinate lat,lon.`);
}

function appendStreetEndpointParams(search, prefix, input) {
  const coordinates = resolveEndpointCoordinates(prefix, input);
  if (!coordinates) {
    throw new Error(
      `Per la modalità drive only serve una ${prefix === "from" ? "origine" : "destinazione"} con coordinate valide. Se scegli una fermata, selezionala dai risultati.`,
    );
  }

  search.set(`${prefix}_lat`, String(coordinates.lat));
  search.set(`${prefix}_lon`, String(coordinates.lon));
}

function resolveEndpointCoordinates(prefix, input) {
  const endpoint = state.selectedEndpoints[prefix];
  if (endpoint && typeof endpoint.lat === "number" && typeof endpoint.lon === "number") {
    return { lat: endpoint.lat, lon: endpoint.lon };
  }

  return parseCoordinateInput(input.value);
}

function parseCoordinateInput(value) {
  const match = value.trim().match(/^(-?\d+(?:\.\d+)?)\s*[,;\s]\s*(-?\d+(?:\.\d+)?)$/);
  if (!match) {
    return null;
  }

  const lat = Number.parseFloat(match[1]);
  const lon = Number.parseFloat(match[2]);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }
  if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
    return null;
  }

  return { lat, lon };
}

function renderSearchResults(prefix, results, input, hiddenInput, container) {
  if (!results.length) {
    container.innerHTML = '<div class="search-result muted">Nessun risultato</div>';
    return;
  }

  container.innerHTML = results
    .map(
      (stop) => `
        <button
          class="search-result"
          type="button"
          data-stop-id="${escapeHtml(stop.id)}"
          data-stop-name="${escapeHtml(stop.name)}"
          data-stop-lat="${stop.latitude ?? ""}"
          data-stop-lon="${stop.longitude ?? ""}"
        >
          <strong>${escapeHtml(stop.name)}</strong>
          <span>${escapeHtml(stop.feed_id)} / ${escapeHtml(stop.local_id)}${stop.code ? ` / ${escapeHtml(stop.code)}` : ""}</span>
        </button>
      `,
    )
    .join("");

  Array.from(container.querySelectorAll(".search-result")).forEach((button) => {
    button.addEventListener("click", () => {
      const latitude = Number.parseFloat(button.dataset.stopLat);
      const longitude = Number.parseFloat(button.dataset.stopLon);
      hiddenInput.value = button.dataset.stopId;
      input.value = button.dataset.stopName;
      setEndpointFromStop(prefix, {
        id: button.dataset.stopId,
        name: button.dataset.stopName,
        latitude: Number.isFinite(latitude) ? latitude : null,
        longitude: Number.isFinite(longitude) ? longitude : null,
      });
      container.innerHTML = "";
    });
  });
}

function renderStats() {
  if (!state.stats) {
    return;
  }

  const { build, realtime, memoization, hpf_overlay: hpfOverlay, street_overlay: streetOverlay } = state.stats;
  const items = [
    ["Static", build.static_cache_hit ? "hit" : "miss"],
    ["Walk", build.walk_cache_hit ? "hit" : "miss"],
    ["HPF", build.hpf_strategy],
    ["HPF nodes", formatCompactNumber(build.hpf_covered_nodes)],
    ["HPF ms", build.timings.hpf_ms],
    ["HPF diff", hpfOverlay?.applied_sequence ? `seq ${hpfOverlay.applied_sequence}` : (hpfOverlay?.enabled ? "idle" : "off")],
    ["Street drive", streetOverlay?.drive?.applied_sequence ? `seq ${streetOverlay.drive.applied_sequence}` : (streetOverlay?.drive?.enabled ? "idle" : "off")],
    ["Street walk", streetOverlay?.walk?.applied_sequence ? `seq ${streetOverlay.walk.applied_sequence}` : (streetOverlay?.walk?.enabled ? "idle" : "off")],
    ["Vehicles", realtime.vehicle_count],
    ["Shadow", realtime.shadow_delta_count],
    ["Memo hits", memoization?.hits ?? 0],
  ];

  dom.statsGrid.innerHTML = items
    .map(
      ([label, value]) => `
        <article class="stat-card">
          <span class="stat-label">${escapeHtml(String(label))}</span>
          <strong class="stat-value">${escapeHtml(String(value))}</strong>
        </article>
      `,
    )
    .join("");
}

function renderQuery() {
  if (!state.query) {
    return;
  }

  if (isStreetRoute(state.query)) {
    renderStreetRoute(state.query);
    return;
  }

  renderTransitQuery(state.query);
}

function renderTransitQuery(summary) {
  dom.itinerarySummary.className = "summary-strip";
  dom.itinerarySummary.innerHTML = `
    <div><strong>${escapeHtml(summary.from.name)}</strong> → <strong>${escapeHtml(summary.to.name)}</strong></div>
    <div>${escapeHtml(summary.departure_time)} → ${escapeHtml(summary.arrival_time)}</div>
    <div>${Math.round(summary.duration_seconds / 60)} min / ${summary.transfers} cambi / ${summary.trace.query_runtime_ms} ms</div>
  `;

  dom.legsContainer.innerHTML = summary.legs
    .map((leg) => {
      const walkDirections = leg.kind === "walk" && Array.isArray(leg.walk_directions)
        ? leg.walk_directions
        : [];
      const walkPreviewStep = walkDirections.find(
        (step) => step.maneuver !== "depart" && step.maneuver !== "arrive",
      ) || walkDirections[0] || null;
      const meta = leg.kind === "walk"
        ? `${Math.round(leg.walk_distance_meters || 0)} m a piedi`
        : `${escapeHtml(leg.route_label || leg.route_id || "linea")} · ${escapeHtml(leg.headsign || "")}`;
      const walkPreview = leg.kind === "walk" && walkPreviewStep
        ? `<div class="walk-preview">${escapeHtml(walkPreviewStep.instruction)}</div>`
        : "";
      const directions = leg.kind === "walk" && walkDirections.length
        ? `
          <ol class="walk-directions">
            ${walkDirections
              .map(
                (step) => `
                  <li class="walk-direction">
                    <span class="walk-direction-text">${escapeHtml(step.instruction)}</span>
                    <span class="walk-direction-distance">${Math.round(step.distance_meters || 0)} m</span>
                  </li>
                `,
              )
              .join("")}
          </ol>
        `
        : "";
      return `
        <article class="leg-card ${leg.kind}">
          <div class="leg-badge">${escapeHtml(leg.kind)}</div>
          <div class="leg-times">${escapeHtml(leg.departure_time)} → ${escapeHtml(leg.arrival_time)}</div>
          <div class="leg-title">${escapeHtml(leg.from_stop.name)} → ${escapeHtml(leg.to_stop.name)}</div>
          ${walkPreview}
          <div class="leg-meta">${meta}</div>
          ${directions}
        </article>
      `;
    })
    .join("");

  dom.traceContainer.innerHTML = `
    <div class="mini-card">
      <strong>Query</strong>
      <span>tot ${summary.trace.query_runtime_ms} ms</span>
      <span>rounds ${summary.trace.timings.rounds_ms} ms</span>
      <span>ped ${summary.trace.timings.pedestrian_lookup_ms} ms</span>
      <span>profile ${summary.trace.timings.profile_lookup_ms} ms</span>
    </div>
    ${summary.trace.coordinate_routing ? `
      <div class="mini-card">
        <strong>DVNI</strong>
        <span>${escapeHtml(summary.trace.coordinate_routing.connector_strategy)}</span>
        <span>seed src ${summary.trace.coordinate_routing.source_seed_count} / seed dst ${summary.trace.coordinate_routing.destination_seed_count}</span>
        <span>asym src ${summary.trace.coordinate_routing.source_asymptotic_connectors} / asym dst ${summary.trace.coordinate_routing.destination_asymptotic_connectors}</span>
      </div>
    ` : ""}
    <table class="trace-grid">
      <thead>
        <tr>
          <th>Round</th>
          <th>Marked</th>
          <th>Lines</th>
          <th>Improvements</th>
          <th>Dest</th>
        </tr>
      </thead>
      <tbody>
        ${summary.trace.rounds
          .map(
            (round) => `
              <tr>
                <td>${round.round}</td>
                <td>${round.marked_stops}</td>
                <td>${round.lines_scanned}</td>
                <td>${round.improvements}</td>
                <td>${escapeHtml(round.destination_time || "-")}</td>
              </tr>
            `,
          )
          .join("")}
      </tbody>
    </table>
  `;

  drawQueryOnMap(summary);
}

function renderStreetRoute(summary) {
  const directions = Array.isArray(summary.directions) ? summary.directions : [];
  const previewStep = directions.find(
    (step) => step.maneuver !== "depart" && step.maneuver !== "arrive",
  ) || directions[0] || null;

  dom.itinerarySummary.className = "summary-strip";
  dom.itinerarySummary.innerHTML = `
    <div><strong>Drive only</strong> · ${escapeHtml(formatStreetEndpoint(summary.from))} → <strong>${escapeHtml(formatStreetEndpoint(summary.to))}</strong></div>
    <div>${formatDistance(summary.distance_meters)} / ${Math.round(summary.duration_seconds / 60)} min</div>
    <div>${summary.trace.query_runtime_ms} ms / ${escapeHtml(summary.trace.strategy)}</div>
  `;

  dom.legsContainer.innerHTML = `
    <article class="leg-card drive">
      <div class="leg-badge">drive</div>
      <div class="leg-title">${escapeHtml(formatStreetEndpoint(summary.from))} → ${escapeHtml(formatStreetEndpoint(summary.to))}</div>
      ${previewStep ? `<div class="walk-preview drive-preview">${escapeHtml(previewStep.instruction)}</div>` : ""}
      <div class="leg-meta">${formatDistance(summary.distance_meters)} in auto · ${Math.round(summary.duration_seconds / 60)} min</div>
      ${directions.length ? `
        <ol class="walk-directions">
          ${directions
            .map(
              (step) => `
                <li class="walk-direction drive-direction">
                  <span class="walk-direction-text">${escapeHtml(step.instruction)}</span>
                  <span class="walk-direction-distance">${Math.round(step.distance_meters || 0)} m</span>
                </li>
              `,
            )
            .join("")}
        </ol>
      ` : ""}
    </article>
  `;

  dom.traceContainer.innerHTML = `
    <div class="mini-card">
      <strong>Street</strong>
      <span>${escapeHtml(summary.mode || "drive")}</span>
      <span>${escapeHtml(summary.trace.strategy)}</span>
    </div>
    <div class="mini-card">
      <strong>Snap</strong>
      <span>src ${Math.round(summary.trace.source_snap_distance_meters || 0)} m</span>
      <span>dst ${Math.round(summary.trace.destination_snap_distance_meters || 0)} m</span>
    </div>
    <div class="mini-card">
      <strong>Search</strong>
      <span>fw ${summary.trace.explored_forward_nodes}</span>
      <span>bw ${summary.trace.explored_backward_nodes}</span>
      <span>${summary.trace.query_runtime_ms} ms</span>
    </div>
  `;

  drawStreetRouteOnMap(summary);
}

function renderRealtime() {
  if (!state.realtime) {
    return;
  }

  const payload = state.realtime;
  const feedCards = payload.feeds && payload.feeds.length
    ? payload.feeds
        .map(
          (feed) => `
            <article class="mini-card">
              <strong>${escapeHtml(feed.feed_id)}</strong>
              <span>trip ${feed.trip_update_entities}</span>
              <span>veh ${feed.vehicle_count}</span>
            </article>
          `,
        )
        .join("")
    : '<div class="mini-card muted">Nessun feed realtime.</div>';

  const vehicleCards = payload.vehicles.length
    ? payload.vehicles
        .slice(0, 8)
        .map(
          (vehicle) => `
            <article class="mini-card">
              <strong>${escapeHtml(vehicle.vehicle_label || vehicle.vehicle_id || vehicle.entity_id)}</strong>
              <span>${escapeHtml(vehicle.trip_id || "trip sconosciuto")}</span>
            </article>
          `,
        )
        .join("")
    : '<div class="mini-card muted">Nessun veicolo disponibile.</div>';

  dom.realtimeContainer.innerHTML = `
    <p>Trip entities: ${payload.trip_update_entities} · Vehicles: ${payload.vehicle_count}</p>
    <p>Last trip refresh: ${escapeHtml(payload.last_trip_refresh || "-")}</p>
    <p>Last vehicle refresh: ${escapeHtml(payload.last_vehicle_refresh || "-")}</p>
    ${payload.last_error ? `<div class="error-box">${escapeHtml(payload.last_error)}</div>` : ""}
    <div class="legs-list">${feedCards}${vehicleCards}</div>
  `;

  drawVehicles(payload.vehicles);
}

function drawQueryOnMap(query) {
  clearMapLayers();

  const bounds = [];
  query.legs.forEach((leg) => {
    if (leg.polyline.length < 2) {
      return;
    }

    const latlngs = leg.polyline.map((point) => [point.lat, point.lon]);
    latlngs.forEach((latlng) => bounds.push(latlng));
    L.polyline(latlngs, {
      color: leg.route_color || (leg.kind === "walk" ? "#4f7f4f" : "#3f62b7"),
      weight: leg.kind === "walk" ? 4 : 5,
      dashArray: leg.kind === "walk" ? "5 7" : null,
      opacity: 0.95,
    }).addTo(state.routeLayer);
  });

  renderEndpointSelections();

  if (bounds.length) {
    state.map.fitBounds(bounds, { padding: [30, 30] });
  }
}

function drawStreetRouteOnMap(route) {
  clearMapLayers();
  const bounds = [];
  if (Array.isArray(route.polyline) && route.polyline.length >= 2) {
    const latlngs = route.polyline.map((point) => [point.lat, point.lon]);
    latlngs.forEach((latlng) => bounds.push(latlng));
    L.polyline(latlngs, {
      color: "#bf5b04",
      weight: 5,
      opacity: 0.95,
    }).addTo(state.routeLayer);
  }

  renderEndpointSelections();

  if (bounds.length) {
    state.map.fitBounds(bounds, { padding: [30, 30] });
  }
}

function drawVehicles(vehicles) {
  state.vehicleLayer.clearLayers();
  vehicles.slice(0, 50).forEach((vehicle) => {
    if (typeof vehicle.latitude !== "number" || typeof vehicle.longitude !== "number") {
      return;
    }

    L.circleMarker([vehicle.latitude, vehicle.longitude], {
      radius: 4,
      color: "#b23b3b",
      fillColor: "#f3d666",
      fillOpacity: 0.9,
      weight: 1,
    })
      .bindPopup(`<strong>${escapeHtml(vehicle.vehicle_label || vehicle.vehicle_id || vehicle.entity_id)}</strong><br/>${escapeHtml(vehicle.trip_id || "trip sconosciuto")}`)
      .addTo(state.vehicleLayer);
  });
}

function clearMapLayers() {
  state.routeLayer.clearLayers();
}

function markRouteStale(message) {
  state.query = null;
  clearMapLayers();
  dom.itinerarySummary.className = "summary-strip";
  dom.itinerarySummary.textContent = message || "Parametri aggiornati. Premi Route.";
  dom.legsContainer.innerHTML = '<div class="empty-state">Nessun itinerario.</div>';
  dom.traceContainer.innerHTML = '<div class="empty-state">Nessun trace disponibile.</div>';
}

function setRefreshStatus(text, busy) {
  dom.refreshStatus.textContent = text;
  dom.refreshStatus.className = `status-pill ${busy ? "busy" : ""}`;
}

function beginBusy() {
  state.busyCount += 1;
  updateBusyIndicator();
  return () => {
    state.busyCount = Math.max(0, state.busyCount - 1);
    updateBusyIndicator();
  };
}

function updateBusyIndicator() {
  dom.topbarProgress.className = `topbar-progress ${state.busyCount > 0 ? "is-active" : ""}`;
}

function getRouteMode() {
  return dom.routeModeSelect.value === "drive-only" ? "drive-only" : "transit";
}

function syncRouteModeUI() {
  const driveOnly = getRouteMode() === "drive-only";
  [dom.dateInput, dom.timeInput, dom.maxTransfersInput].forEach((input) => {
    input.disabled = driveOnly;
    const group = input.closest(".field-group");
    if (group) {
      group.classList.toggle("is-disabled", driveOnly);
    }
  });
}

function isStreetRoute(payload) {
  return payload && Array.isArray(payload.polyline) && !Array.isArray(payload.legs);
}

function formatStreetEndpoint(endpoint) {
  if (!endpoint || typeof endpoint.lat !== "number" || typeof endpoint.lon !== "number") {
    return "punto sconosciuto";
  }

  return formatCoordinateValue(endpoint.lat, endpoint.lon);
}

function formatDistance(distanceMeters) {
  if (!Number.isFinite(distanceMeters)) {
    return "0 m";
  }
  if (distanceMeters >= 1000) {
    return `${(distanceMeters / 1000).toFixed(1)} km`;
  }
  return `${Math.round(distanceMeters)} m`;
}

function formatCoordinateValue(latitude, longitude) {
  return `${latitude.toFixed(6)}, ${longitude.toFixed(6)}`;
}

function formatCompactNumber(value) {
  return Number(value).toLocaleString("it-IT");
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error(payload?.error || `Request failed with ${response.status}`);
  }
  return payload;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}