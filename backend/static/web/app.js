
// Smart Parking System
// Frontend Simulation Demo
// 31261 Internetworking Project

// 1. Simulated parking sensor data
const parkingSpaces = [
    {
        id: "A01",
        spotId: 1001,
        occupied: false,
        online: true,
        lastUpdate: new Date()
    },
    {
        id: "A02",
        spotId: 1002,
        occupied: true,
        online: true,
        lastUpdate: new Date()
    },
    {
        id: "A03",
        spotId: 1003,
        occupied: false,
        online: false,
        lastUpdate: null
    }
];

// 2. Determine parking status
function getParkingStatus(space) {
    // Live mode: the backend decides the status (it also handles timeouts and overrides)
    if (space.status) {
        return space.status.toLowerCase() === "available" ? "available"
             : space.status.toLowerCase() === "occupied" ? "occupied"
             : "unknown";
    }

    if (!space.online) {
        return "unknown";
    }

    return space.occupied ? "occupied" : "available";
}

// 3. Update Parking Dashboard
function updateDashboard() {
    const parkingGrid = document.getElementById("parkingGrid");

    parkingGrid.innerHTML = "";

    let available = 0;
    let occupied = 0;
    let unknown = 0;

    parkingSpaces.forEach(space => {
        const status = getParkingStatus(space);

        if (status === "available") available++;
        if (status === "occupied") occupied++;
        if (status === "unknown") unknown++;

        const card = document.createElement("div");
        card.className = "parking-card " + status;

        const title = document.createElement("h3");
        title.textContent = space.id;

        const badge = document.createElement("span");
        badge.className = "status-badge";
        badge.textContent =
            status.charAt(0).toUpperCase() + status.slice(1);

        card.appendChild(title);
        card.appendChild(badge);
        parkingGrid.appendChild(card);
    });

    document.getElementById("availableCount").textContent = available;
    document.getElementById("occupiedCount").textContent = occupied;
    document.getElementById("unknownCount").textContent = unknown;
}

// 4. Update Admin View
function updateAdminView() {
    const tableBody = document.getElementById("adminTableBody");

    tableBody.innerHTML = "";

    parkingSpaces.forEach(space => {
        const row = document.createElement("tr");

        const sensorId = document.createElement("td");
        sensorId.textContent = space.id;

        const parkingStatus = document.createElement("td");
        parkingStatus.textContent = getParkingStatus(space);

        const connection = document.createElement("td");
        connection.textContent = space.online ? "Online" : "Offline";
        connection.className =
            "connection " + (space.online ? "online" : "offline");

        const lastUpdate = document.createElement("td");
        lastUpdate.textContent = space.lastUpdate
            ? space.lastUpdate.toLocaleTimeString()
            : "No data";

        row.appendChild(sensorId);
        row.appendChild(parkingStatus);
        row.appendChild(connection);
        row.appendChild(lastUpdate);

        tableBody.appendChild(row);
    });
}

// 5. Navigation between Dashboard and Admin View
const tabs = document.querySelectorAll(".tab");
const pages = document.querySelectorAll(".page");

tabs.forEach(tab => {
    tab.addEventListener("click", () => {
        tabs.forEach(t => t.classList.remove("active"));
        pages.forEach(p => p.classList.remove("active"));

        tab.classList.add("active");

        const pageId = tab.dataset.page;
        document.getElementById(pageId).classList.add("active");
    });
});

// 6. Simulated sensor updates
// These are generated locally, not received from ESP32.
let simulationStep = 0;

function simulateSensorUpdates() {
    simulationStep++;

    // Change the first parking space every 10 seconds
    if (simulationStep % 2 === 0) {
        parkingSpaces[0].occupied = !parkingSpaces[0].occupied;
    }

    // Change the second parking space every 15 seconds
    if (simulationStep % 3 === 0) {
        parkingSpaces[1].occupied = !parkingSpaces[1].occupied;
    }

    // Simulated heartbeat from online sensors
    parkingSpaces.forEach(space => {
        if (space.online) {
            space.lastUpdate = new Date();
        }
    });

    updateDashboard();
    updateAdminView();
}

// 7. Live data from the backend
// The page first tries to reach the backend. If it answers, the dashboard and
// admin view show real data from the MQTT -> backend -> database pipeline and
// are updated instantly over a WebSocket. If not, the original local
// simulation above runs unchanged.
//
// Backend address, in order of preference:
//   1. ?api=http://host:8000 in the page URL
//   2. the same server, when the page is served by the backend (/app/)
//   3. http://localhost:8000
function getApiBase() {
    const param = new URLSearchParams(window.location.search).get("api");
    if (param) return param.replace(/\/$/, "");
    if (window.location.pathname.startsWith("/app")) return window.location.origin;
    return "http://localhost:8000";
}

const API_BASE = getApiBase();
let simulationTimer = null;
let pollTimer = null;
let socket = null;

function applyBackendSpaces(spaces) {
    parkingSpaces.length = 0;
    spaces.forEach(s => parkingSpaces.push(fromBackend(s)));
    updateDashboard();
    updateAdminView();
}

function applyBackendSpace(s) {
    const i = parkingSpaces.findIndex(p => p.id === s.space_id);
    if (i >= 0) parkingSpaces[i] = fromBackend(s);
    else parkingSpaces.push(fromBackend(s));
    updateDashboard();
    updateAdminView();
}

function fromBackend(s) {
    return {
        id: s.space_id,
        spotId: s.spot_id,
        status: s.status,                    // Available | Occupied | Unknown
        occupied: s.status === "Occupied",
        online: !!s.online,
        lastUpdate: s.last_seen ? new Date(s.last_seen) : null
    };
}

function setMode(live) {
    const mode = document.querySelector(".mode");
    if (mode) mode.textContent = live ? "Live Data (Backend)" : "Simulation Mode";
    const note = document.querySelector("footer p:last-child");
    if (note) note.textContent = live
        ? "Live data from the sensing node via MQTT, backend and database"
        : "Demo interface using simulated parking data";
}

async function fetchSpaces() {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), 2500);
    try {
        const res = await fetch(API_BASE + "/api/spaces", { signal: controller.signal });
        if (!res.ok) throw new Error("HTTP " + res.status);
        return (await res.json()).spaces;
    } finally {
        clearTimeout(t);
    }
}

function connectSocket() {
    const wsUrl = API_BASE.replace(/^http/, "ws") + "/ws";
    socket = new WebSocket(wsUrl);
    socket.onopen = () => {
        if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
    };
    socket.onmessage = event => {
        const msg = JSON.parse(event.data);
        if (msg.type === "snapshot") applyBackendSpaces(msg.spaces);
        if (msg.type === "space_update") applyBackendSpace(msg.space);
    };
    socket.onclose = () => {
        // Fall back to polling every 5 s and try the WebSocket again
        if (!pollTimer) {
            pollTimer = setInterval(() => fetchSpaces().then(applyBackendSpaces).catch(() => {}), 5000);
        }
        setTimeout(connectSocket, 5000);
    };
}

function startSimulation() {
    setMode(false);
    updateDashboard();
    updateAdminView();
    // Simulate new readings every 5 seconds
    simulationTimer = setInterval(simulateSensorUpdates, 5000);
}

// 8. Initialise application
updateDashboard();
updateAdminView();

fetchSpaces()
    .then(spaces => {
        setMode(true);
        applyBackendSpaces(spaces);
        connectSocket();
    })
    .catch(() => startSimulation());
