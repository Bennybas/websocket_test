import json
import logging
import os
from datetime import datetime
from typing import Optional, List, Dict, Any, Set
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import asyncio

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Chryselys WebSocket Data API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


# WebSocket connection manager
class ConnectionManager:
    """Manages active WebSocket connections."""
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.heartbeat_task: Optional[asyncio.Task] = None
    
    async def connect(self, websocket: WebSocket):
        """Accept a new WebSocket connection."""
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")
        
        # Start heartbeat if first connection
        if len(self.active_connections) == 1 and not self.heartbeat_task:
            self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
    
    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection."""
        self.active_connections.discard(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
        
        # Stop heartbeat if no connections
        if len(self.active_connections) == 0 and self.heartbeat_task:
            self.heartbeat_task.cancel()
            self.heartbeat_task = None
    
    async def _heartbeat_loop(self):
        """Send periodic ping messages to keep connection alive."""
        while self.active_connections:
            try:
                await asyncio.sleep(30)
                if self.active_connections:
                    dead_connections = []
                    for connection in self.active_connections.copy():
                        try:
                            await connection.send_json({"type": "ping", "timestamp": datetime.now().isoformat()})
                        except Exception as e:
                            logger.warning(f"Error sending heartbeat: {e}")
                            dead_connections.append(connection)
                    
                    for conn in dead_connections:
                        self.disconnect(conn)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
    
    async def broadcast_payloads(self, payloads: List[Dict[str, Any]]):
        """Broadcast payload data directly to all connected clients via WebSocket."""
        if not self.active_connections:
            logger.info(f"No active WebSocket connections. Received {len(payloads)} payload(s)")
            return
        
        if not payloads:
            logger.warning("No payloads to broadcast")
            return
        
        message = {
            "type": "data_ready",
            "payloads": payloads,
            "payloads_count": len(payloads),
            "timestamp": datetime.now().isoformat()
        }
        
        dead_connections = []
        for connection in self.active_connections.copy():
            try:
                await connection.send_json(message)
                logger.info(f"Broadcasted {len(payloads)} payload(s) via WebSocket")
            except Exception as e:
                logger.warning(f"Error broadcasting to connection: {e}")
                dead_connections.append(connection)
        
        for conn in dead_connections:
            self.disconnect(conn)


# Global connection manager
manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time data notifications."""
    await manager.connect(websocket)
    try:
        await websocket.send_json({
            "type": "connected",
            "message": "WebSocket connected successfully",
            "timestamp": datetime.now().isoformat()
        })
        
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
                try:
                    message = json.loads(data)
                    msg_type = message.get("type")
                    
                    if msg_type == "pong":
                        logger.debug("Received pong from client")
                    elif msg_type == "ack":
                        logger.info(f"Received acknowledgment: {message.get('message', '')}")
                    elif msg_type == "producer_connected":
                        logger.info(f"Producer connected: {message.get('message', '')}")
                    elif msg_type == "payloads_ready":
                        payloads = message.get("payloads", [])
                        payloads_count = message.get("payloads_count", 0)
                        logger.info(f"Received {payloads_count} payload(s) from producer")
                        
                        await manager.broadcast_payloads(payloads)
                    else:
                        logger.debug(f"Received unknown message type: {msg_type}")
                        
                except json.JSONDecodeError:
                    logger.debug(f"Received non-JSON message: {data}")
            except asyncio.TimeoutError:
                continue
            except WebSocketDisconnect:
                break
                
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        manager.disconnect(websocket)


@app.post("/final_output")
async def final_output(processed_data: List[Dict[str, Any]] = Body(...)):
    """Receive processed data acknowledgment."""
    try:
        if not processed_data:
            logger.warning("Received empty processed_data")
            return {
                "success": True,
                "message": "No data received",
                "processed_count": 0
            }
        
        return {
            "success": True,
            "message": "Processed data received successfully",
            "processed_count": len(processed_data),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error in final_output: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("API_PORT", 8000)))
