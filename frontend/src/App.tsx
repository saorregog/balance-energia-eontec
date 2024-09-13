import "./App.css";

import { createBrowserRouter, RouterProvider } from "react-router-dom";
import Login from "./pages/Login";
import ETL from "./pages/ETL";

const router = createBrowserRouter([
  {
    path: "/",
    element: <Login />,
  },
  {
    path: "/etl",
    element: <ETL />,
  },
]);

export default function App() {
  return <RouterProvider router={router} />;
}
