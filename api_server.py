#!/usr/bin/env python3
"""
REST API & WebSocket Server
Serves correlation data from time series DB to frontend dashboard
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

INFLUX_USER = os.getenv('INFLUX_USER')
INFLUX_PASSWORD = os.getenv('INFLUX_PASSWORD')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="EUR/USD Correlation API",
    description="Real-time streaming correlation analysis",
    version="1.0.0"
)

# CORS configuration (for local frontend development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["localhost", "127.0.0.1", "*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# DATA SIMULATION (replace with real DB queries in production)
# ============================================================================

class DataStore:
    """In-memory data store (replace with InfluxDB/TimescaleDB in production)"""
    
    def __init__(self):
        self.latest_prices = {}
        self.correlations = {}
        self.anomalies = []
        self.regimes = []
    
    def update_prices(self, data: Dict) -> None:
        """Update latest price data"""
        self.latest_prices = {
            **self.latest_prices,
            **data,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def update_correlations(self, window_h: int, correlations: Dict) -> None:
        """Update correlation data for a window"""
        if f'{window_h}h' not in self.correlations:
            self.correlations[f'{window_h}h'] = []
        
        self.correlations[f'{window_h}h'].append({
            'timestamp': datetime.utcnow().isoformat(),
            'data': correlations
        })
        
        # Keep only last 1000 entries per window
        if len(self.correlations[f'{window_h}h']) > 1000:
            self.correlations[f'{window_h}h'] = self.correlations[f'{window_h}h'][-1000:]
    
    def record_anomaly(self, anomaly: Dict) -> None:
        """Record a detected anomaly"""
        self.anomalies.append({
            'timestamp': datetime.utcnow().isoformat(),
            **anomaly
        })
        
        # Keep only last 100 anomalies
        if len(self.anomalies) > 100:
            self.anomalies = self.anomalies[-100:]
    
    def record_regime(self, regime: str, strength: float) -> None:
        """Record regime change"""
        self.regimes.append({
            'timestamp': datetime.utcnow().isoformat(),
            'regime': regime,
            'strength': strength
        })
        
        # Keep only last 1000 entries
        if len(self.regimes) > 1000:
            self.regimes = self.regimes[-1000:]


data_store = DataStore()

# ============================================================================
# ROUTES
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "eur-usd-correlation-api"
    }


@app.get("/api/prices")
async def get_latest_prices():
    """Get latest price data for all instruments"""
    if not data_store.latest_prices:
        raise HTTPException(status_code=404, detail="No price data available")
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "data": data_store.latest_prices
    }


@app.get("/api/correlations/{window_h}")
async def get_correlations(window_h: int, limit: int = 100):
    """
    Get correlation data for a specific time window.
    
    Args:
        window_h: Window size in hours (1, 4, or 24)
        limit: Number of recent entries to return
    
    Returns:
        List of correlation matrices
    """
    key = f'{window_h}h'
    
    if key not in data_store.correlations or not data_store.correlations[key]:
        raise HTTPException(status_code=404, detail=f"No correlation data for {window_h}h window")
    
    entries = data_store.correlations[key][-limit:]
    
    return {
        "window_h": window_h,
        "count": len(entries),
        "data": entries
    }


@app.get("/api/correlations/{window_h}/latest")
async def get_latest_correlations(window_h: int):
    """Get the most recent correlation matrix"""
    key = f'{window_h}h'
    
    if key not in data_store.correlations or not data_store.correlations[key]:
        raise HTTPException(status_code=404, detail=f"No correlation data for {window_h}h window")
    
    latest = data_store.correlations[key][-1]
    
    return {
        "window_h": window_h,
        "timestamp": latest['timestamp'],
        "correlations": latest['data']
    }


@app.get("/api/anomalies")
async def get_anomalies(limit: int = 20):
    """Get recent detected anomalies"""
    return {
        "count": len(data_store.anomalies),
        "data": data_store.anomalies[-limit:]
    }


@app.get("/api/regimes")
async def get_regimes(limit: int = 50):
    """Get recent regime changes"""
    return {
        "count": len(data_store.regimes),
        "data": data_store.regimes[-limit:]
    }


@app.get("/api/summary")
async def get_summary():
    """Get overall market summary"""
    latest_price = data_store.latest_prices
    latest_1h = data_store.correlations.get('1h', [{}])[-1] if '1h' in data_store.correlations else {}
    latest_regime = data_store.regimes[-1] if data_store.regimes else {}
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "price_data": latest_price,
        "latest_correlations_1h": latest_1h.get('data', {}),
        "current_regime": latest_regime,
        "recent_anomalies": len([a for a in data_store.anomalies if 
                                datetime.fromisoformat(a['timestamp']) > datetime.utcnow() - timedelta(hours=1)])
    }


# ============================================================================
# WEBSOCKET STREAMING
# ============================================================================

class ConnectionManager:
    """Manage WebSocket connections"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Client connected. Active connections: {len(self.active_connections)}")
    
    async def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"Client disconnected. Active connections: {len(self.active_connections)}")
    
    async def broadcast(self, message: dict):
        """Send message to all connected clients"""
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting: {e}")


manager = ConnectionManager()


@app.websocket("/ws/prices")
async def websocket_prices(websocket: WebSocket):
    """
    WebSocket endpoint for real-time price updates.
    Clients receive latest prices as they're updated.
    """
    await manager.connect(websocket)
    
    try:
        while True:
            # Receive commands from client (optional)
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                command = json.loads(data)
                logger.info(f"Received command: {command}")
            except asyncio.TimeoutError:
                pass  # No new commands, continue
            
            # Send latest price data
            if data_store.latest_prices:
                await websocket.send_json({
                    "type": "price_update",
                    "timestamp": datetime.utcnow().isoformat(),
                    "data": data_store.latest_prices
                })
            
            # Small delay to avoid hammering
            await asyncio.sleep(1)
    
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
        logger.info("Client disconnected")


@app.websocket("/ws/correlations/{window_h}")
async def websocket_correlations(websocket: WebSocket, window_h: int):
    """
    WebSocket endpoint for real-time correlation updates.
    Client specifies window size (1, 4, 24).
    """
    await manager.connect(websocket)
    
    try:
        while True:
            key = f'{window_h}h'
            
            if key in data_store.correlations and data_store.correlations[key]:
                latest = data_store.correlations[key][-1]
                
                await websocket.send_json({
                    "type": "correlation_update",
                    "window_h": window_h,
                    "timestamp": latest['timestamp'],
                    "correlations": latest['data']
                })
            
            # Update frequency: every 5 seconds
            await asyncio.sleep(5)
    
    except WebSocketDisconnect:
        await manager.disconnect(websocket)


@app.websocket("/ws/stream")
async def websocket_combined(websocket: WebSocket):
    """
    Combined stream: prices + correlations + anomalies + regimes
    One connection for everything.
    """
    await manager.connect(websocket)
    
    try:
        update_counter = 0
        
        while True:
            update_counter += 1
            
            message = {
                "type": "combined_update",
                "timestamp": datetime.utcnow().isoformat(),
                "sequence": update_counter,
                "data": {
                    "prices": data_store.latest_prices,
                    "correlations_1h": data_store.correlations.get('1h', [{}])[-1].get('data', {}),
                    "correlations_4h": data_store.correlations.get('4h', [{}])[-1].get('data', {}),
                    "current_regime": data_store.regimes[-1] if data_store.regimes else None,
                    "recent_anomalies": data_store.anomalies[-5:] if data_store.anomalies else []
                }
            }
            
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.error(f"Error sending message: {e}")
                break
            
            # Update every 2 seconds
            await asyncio.sleep(2)
    
    except WebSocketDisconnect:
        await manager.disconnect(websocket)


# ============================================================================
# SIMULATION DATA ENDPOINT (for testing)
# ============================================================================

@app.post("/api/simulate/prices")
async def simulate_prices(data: Dict):
    """Simulate incoming price data (for testing)"""
    data_store.update_prices(data)
    await manager.broadcast({
        "type": "price_update",
        "timestamp": datetime.utcnow().isoformat(),
        "data": data_store.latest_prices
    })
    return {"status": "ok", "message": "Price data updated"}


@app.post("/api/simulate/correlations")
async def simulate_correlations(window_h: int, correlations: Dict):
    """Simulate correlation calculation"""
    data_store.update_correlations(window_h, correlations)
    return {"status": "ok", "message": f"Correlations updated for {window_h}h"}


@app.post("/api/simulate/anomaly")
async def simulate_anomaly(anomaly: Dict):
    """Simulate detected anomaly"""
    data_store.record_anomaly(anomaly)
    return {"status": "ok", "message": "Anomaly recorded"}


# ============================================================================
# STARTUP/SHUTDOWN
# ============================================================================

@app.on_event("startup")
async def startup_event():
    logger.info("🚀 EUR/USD Correlation API starting up...")
    logger.info("  Health check: GET /health")
    logger.info("  API docs: http://localhost:8000/docs")
    logger.info("  WebSocket (combined): ws://localhost:8000/ws/stream")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("🛑 API shutting down")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Run with: uvicorn api_server:app --reload --host 0.0.0.0 --port 8000
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
