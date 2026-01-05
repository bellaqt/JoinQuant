# -------- runtime base --------
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 8080
EXPOSE 8081

# -------- build with node --------
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS with-node
RUN apt-get update \
 && apt-get install -y curl \
 && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
 && apt-get install -y nodejs

# -------- restore & build --------
FROM with-node AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src

# copy only project files first (for cache + correct restore)
COPY JoinQuant.Server/JoinQuant.Server.csproj JoinQuant.Server/
COPY joinquant.client/joinquant.client.esproj joinquant.client/

# restore explicitly
RUN dotnet restore JoinQuant.Server/JoinQuant.Server.csproj

# now copy the rest
COPY . .

WORKDIR /src/JoinQuant.Server
RUN dotnet build JoinQuant.Server.csproj -c $BUILD_CONFIGURATION -o /app/build

# -------- publish --------
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish JoinQuant.Server.csproj \
    -c $BUILD_CONFIGURATION \
    -o /app/publish \
    /p:UseAppHost=false

# -------- final image --------
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "JoinQuant.Server.dll"]
