import io from "socket.io-client";

const backendURL = "http://localhost:8000";

const socket = io(backendURL, {
  autoConnect: false,
});

export default socket;
