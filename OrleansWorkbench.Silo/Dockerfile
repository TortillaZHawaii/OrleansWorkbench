FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["OrleansWorkbench.Silo/OrleansWorkbench.Silo.csproj", "OrleansWorkbench.Silo/"]
RUN dotnet restore "OrleansWorkbench.Silo/OrleansWorkbench.Silo.csproj"
COPY . .
WORKDIR "/src/OrleansWorkbench.Silo"
RUN dotnet build "OrleansWorkbench.Silo.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "OrleansWorkbench.Silo.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "OrleansWorkbench.Silo.dll"]
