import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useForm } from "react-hook-form";
import z from "zod";
import { zodResolver } from "@hookform/resolvers/zod";

import { Button } from "@/components/ui/button";
import {
  Card,
  CardHeader,
  CardTitle,
  CardContent,
  CardFooter,
} from "@/components/ui/card";
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";

import { Eye, EyeOff } from "lucide-react";

const loginSchema = z.object({
  username: z.string().min(1, { message: "El nombre de usuario es requerido" }),
  password: z.string().min(1, { message: "La contraseña es requerida" }),
});

type LoginForm = z.infer<typeof loginSchema>;

export default function Login() {
  const [showPassword, setShowPassword] = useState<boolean>(false);
  const [error, setError] = useState<string>("");

  const navigate = useNavigate();

  const form = useForm<LoginForm>({
    resolver: zodResolver(loginSchema),
    defaultValues: {
      username: "",
      password: "",
    },
  });

  const cleanError = () => setError("");

  const togglePassword = () => setShowPassword(!showPassword);

  const onSubmit = async (values: LoginForm) => {
    if (values.username === "saorregog" && values.password === "123456") {
      navigate("/etl");
      return;
    }

    setError("Usuario y/o contraseña incorrectos");
  };

  return (
    <div className="h-screen flex items-center justify-center bg-background px-4 py-12 sm:px-6 lg:px-8">
      <Card className="w-full max-w-80 border-[#808080] bg-[#f0f0f0] shadow-[0_0_10px_0_rgba(0,0,0,0.5)]">
        <CardHeader className="border-b border-[#808080] bg-muted px-6 py-4">
          <CardTitle className="text-2xl font-bold">Login</CardTitle>
        </CardHeader>
        <CardContent className="px-6 pt-6 pb-10">
          <div>
            <Form {...form}>
              <form
                id="login"
                onSubmit={form.handleSubmit(onSubmit)}
                className="space-y-8"
              >
                {/* Username */}
                <FormField
                  control={form.control}
                  name="username"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Usuario</FormLabel>
                      <FormControl>
                        <Input
                          {...field}
                          onInput={cleanError}
                          className="border border-[#808080] bg-[#ffffff] px-2 py-1 focus:border-[#0000ff] text-md"
                        />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />

                {/* Password */}
                <FormField
                  control={form.control}
                  name="password"
                  render={({ field }) => (
                    <FormItem className="relative">
                      <FormLabel>Contraseña</FormLabel>
                      <FormControl>
                        <Input
                          type={showPassword ? "text" : "password"}
                          {...field}
                          onInput={cleanError}
                          className="border border-[#808080] bg-[#ffffff] px-2 py-1 focus:border-[#0000ff] text-md pr-[45px]"
                        />
                      </FormControl>
                      {showPassword ? (
                        <Eye
                          onClick={togglePassword}
                          className="absolute right-4 top-8 cursor-pointer"
                        />
                      ) : (
                        <EyeOff
                          onClick={togglePassword}
                          className="absolute right-4 top-8 cursor-pointer"
                        />
                      )}
                      <FormMessage />
                    </FormItem>
                  )}
                />

                {error && (
                  <p className="text-sm font-medium text-destructive text-center">
                    {error}
                  </p>
                )}
              </form>
            </Form>
          </div>
        </CardContent>
        <CardFooter className="border-t border-[#808080] bg-muted px-6 py-4">
          <Button
            form="login"
            type="submit"
            variant="outline"
            className="bg-white"
          >
            Ingresar
          </Button>
        </CardFooter>
      </Card>
    </div>
  );
}
