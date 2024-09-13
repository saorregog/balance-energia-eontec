import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import socket from "@/socket";

import {
  Accordion,
  AccordionItem,
  AccordionTrigger,
  AccordionContent,
} from "@/components/ui/accordion";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from "@/components/ui/dropdown-menu";
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from "@/components/ui/table";

import { Check, DatabaseZap, LoaderCircle, Server, X } from "lucide-react";

const thousandsFormatter = new Intl.NumberFormat("es-CO");

export default function ETL() {
  const [isServerUp, setIsServerUp] = useState(false);
  const [isConnected, setIsConnected] = useState(socket.connected);
  const [loading, setLoading] = useState(false);
  const [eventsMessages, setEventsMessages] = useState([]);
  const [energyBalance, setEnergyBalance] = useState([]);

  const navigate = useNavigate();

  const checkServer = async () => {
    try {
      const response = await fetch("http://localhost:8000");

      if (response.status === 200) {
        setIsServerUp(true);
      }
    } catch (error) {
      console.log(error.message);
    }
  };

  const connect = () => socket.connect();
  const startETL = () => {
    setLoading(true);
    setEventsMessages([]);
    setEnergyBalance([]);

    socket.emit("start_etl");
  };
  const sendReport = () => {
    setLoading(true);

    socket.emit("send_report");
  };

  const onConnect = () => setIsConnected(true);
  const onEventsMessages = (message) => {
    setEventsMessages((prev) => [...prev, message]);
  };
  const onStopProcessing = () => setLoading(false);
  const onEnergyBalance = (message) => {
    setEnergyBalance(JSON.parse(message));
  };
  const onDenyReport = () => {
    setEventsMessages([]);
    setEnergyBalance([]);
  };

  useEffect(() => {
    // Check if server is up
    checkServer();
  }, []);

  useEffect(() => {
    if (isServerUp) {
      connect();

      // Bind events and handlers
      socket.on("connect", onConnect);
      socket.on("events_messages", onEventsMessages);
      socket.on("stop_processing", onStopProcessing);
      socket.on("energy_balance", onEnergyBalance);
    }

    return () => {
      // Unbind events and handlers when App component is unmounted
      socket.off("connect", onConnect);
      socket.off("events_messages", onEventsMessages);
      socket.off("stop_processing", onStopProcessing);
      socket.off("energy_balance", onEnergyBalance);
    };
  }, [isServerUp]);

  const logOut = () => navigate("/");

  return (
    <div className="min-h-screen bg-background">
      <header className="flex h-16 items-center justify-between border-b bg-muted px-4 md:px-6">
        <a href="#" className="flex items-center gap-2 font-semibold">
          <DatabaseZap className="h-6 w-6" />
          <span>Balance Energía</span>
        </a>
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Server className="h-4 w-4" />
          {isServerUp ? (
            <span className="font-medium text-green-700">{`Servidor Activo${
              isConnected ? " (Conectado)" : ""
            }`}</span>
          ) : (
            <span className="font-medium text-red-700">Servidor Inactivo</span>
          )}
        </div>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Avatar className="w-10 h-10 cursor-pointer hover:ring-4 hover:ring-white">
              <AvatarFallback className="bg-indigo-700 text-white font-medium tracking-widest">
                SO
              </AvatarFallback>
            </Avatar>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="min-w-24">
            <DropdownMenuItem
              onClick={logOut}
              className="text-xs cursor-pointer"
            >
              Cerrar Sesión
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </header>

      <main className="px-4 py-8 md:p-6">
        <div className="grid gap-6">
          <Button variant="outline" onClick={startETL} className="w-32">
            Ejecutar ETL
          </Button>

          {eventsMessages.length > 0 && (
            <div className="grid gap-4">
              <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold">Registro de Eventos</h2>
              </div>
              <div className="grid gap-2 rounded-lg border bg-background p-4">
                <div className="grid gap-2">
                  {eventsMessages.map((message, index) => (
                    <div
                      key={index}
                      className="flex items-center gap-2 text-sm"
                    >
                      {message.status === "success" ? (
                        <Check className="h-4 w-4 text-green-700" />
                      ) : (
                        <X className="h-4 w-4 text-red-700" />
                      )}
                      <span
                        className={
                          message.status === "success"
                            ? "text-green-700"
                            : "text-red-700"
                        }
                      >
                        {message.content}
                      </span>
                    </div>
                  ))}

                  {loading && (
                    <div className="flex items-center gap-2 text-sm">
                      <LoaderCircle className="h-4 w-4 animate-spin" />
                      <span className="font-medium">Cargando...</span>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {energyBalance.length > 0 && (
            <Accordion type="single" collapsible>
              <AccordionItem value="results">
                <AccordionTrigger>
                  <div className="flex items-center justify-between">
                    <h2 className="text-xl font-bold">Resultado del Balance</h2>
                  </div>
                </AccordionTrigger>
                <AccordionContent>
                  <div className="rounded-lg border bg-background p-4">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          {Object.keys(energyBalance[0]).map((key, index) => (
                            <TableHead key={index}>{key}</TableHead>
                          ))}
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {energyBalance.map((row, index) => (
                          <TableRow key={index}>
                            <TableCell>{row["Fecha"]}</TableCell>
                            <TableCell>{row["Codigo"]}</TableCell>
                            <TableCell>
                              {thousandsFormatter.format(row["Balance (kWh)"])}
                            </TableCell>
                            <TableCell>
                              {thousandsFormatter.format(
                                row["Compromisos (COP)"]
                              )}
                            </TableCell>
                            <TableCell
                              className={
                                row["Operacion"] === "Vender"
                                  ? "text-green-700"
                                  : "text-red-700"
                              }
                            >
                              {row["Operacion"]}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </AccordionContent>
              </AccordionItem>
            </Accordion>
          )}

          {energyBalance.length > 0 && (
            <div className="mt-4 flex justify-end gap-2">
              <Button variant="outline" onClick={sendReport}>
                Enviar Informe
              </Button>
              <Button variant="destructive" onClick={onDenyReport}>
                Rechazar Informe
              </Button>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
