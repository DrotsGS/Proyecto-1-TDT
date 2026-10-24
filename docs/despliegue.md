# Despliegue de DFSha

El mismo código corre de tres formas, y solo cambian las variables de
entorno:

| Forma | Para qué | Cómo |
|---|---|---|
| Un proceso por nodo en tu PC | desarrollo y pruebas | [README, sección 2](../README.md#2-levantar-los-nodos-en-tu-pc) |
| Docker en una sola máquina | probar la imagen y la red | sección 1 de este documento |
| Una instancia EC2 por nodo | la entrega | [README, AWS](../README.md#desplegarlo-en-aws-academy) |

---

## 1. Docker en una sola máquina

`docker-compose.yml` levanta 3 ControlNodes con Raft y 4 DataNodes en la
misma máquina, cada uno en su contenedor, con mTLS:

```bash
python scripts/gen_certs.py          # crea certs/ (una vez)
docker compose up --build -d
docker compose run --rm cliente python src/client/cli.py login drots --password dfsha
docker compose run --rm cliente python src/client/cli.py mkdir /docs
docker compose stop namenode-1       # el cliente sigue funcionando
docker compose down -v
```

Sin mTLS: `DFSHA_TLS=0 docker compose up --build -d`.

---

## 2. Notas sobre el despliegue en AWS

**Por qué tres Security Groups.** Cada rol abre solo lo que necesita, y
las reglas apuntan a **grupos** y no a IPs. Así, una instancia nueva hereda
los permisos de su grupo sin tocar nada. Si solo se abre el 50051, `ping`
funciona pero `put` falla, porque el cliente habla **directo** con los
DataNodes: es la separación de planos del diseño.

**Por qué IPs privadas.** El cliente corre en una instancia dentro de la
misma VPC. Así ningún puerto del sistema queda expuesto a Internet: solo
el 22, para entrar a las terminales.

**Por qué no Let's Encrypt, como en el taller.** Let's Encrypt certifica
un dominio público ante los navegadores. Aquí se autentican los nodos
entre sí, por IP privada y en los dos sentidos (mTLS), y para eso sirve
una CA propia (`scripts/gen_certs.py`). La alternativa con `openssl`:

```bash
mkdir -p deploy/certs && cd deploy/certs
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout ca.key -out ca.crt -subj "/CN=dfsha-ca"
for rol in nodo cliente; do
  openssl req -newkey rsa:2048 -nodes -keyout $rol.key -out $rol.csr \
    -subj "/CN=$rol" -addext "subjectAltName=DNS:dfsha,DNS:localhost"
  openssl x509 -req -in $rol.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out $rol.crt -days 365 -copy_extensions copy
done
```

**El Learner Lab apaga las instancias** al cerrar la sesión. Al volver se
conservan las IPs privadas, y el namespace (journal de Raft) y los bloques
siguen en disco: basta con volver a correr los `docker compose up -d`.
También limita la cuenta a unas 9 instancias, contando las detenidas.

---

## 3. Variables de entorno

| Variable | Quién | Para qué |
|---|---|---|
| `NODE_ID` | todos | nombre del nodo |
| `PORT` | ControlNode, DataNode | puerto gRPC |
| `RAFT_SELF`, `RAFT_PEERS` | ControlNode | direcciones de Raft; sin `RAFT_SELF` corre solo |
| `RAFT_DIR` | ControlNode | journal de Raft |
| `RAFT_PASSWORD` | ControlNode | cifra el tráfico entre ControlNodes |
| `NAMENODES_CLIENTE` | ControlNode | `raft=cliente,...` para que `Ping` indique el líder |
| `NAMENODE_ADDR` | DataNode, cliente | los ControlNodes, separados por coma |
| `ADVERTISE_ADDR` | DataNode | dirección por la que lo alcanzan los clientes |
| `DATA_DIR` | DataNode | carpeta de bloques |
| `DFSHA_TLS`, `DFSHA_CERTS`, `DFSHA_CERT` | todos | mTLS |
| `DFSHA_SECRET` | ControlNode | firma los tokens de usuario |
| `DFSHA_BLOCK_SECRET` | ControlNode, DataNode | firma los tokens de bloque |
| `DFSHA_REPLICATION` | ControlNode | factor de réplica (3) |
| `DFSHA_BLOCK_SIZE` | ControlNode | tamaño de bloque (128 MB) |
| `LEASE_TTL`, `TIMEOUT_DATANODE` | ControlNode | 300 s y 30 s |
| `DFSHA_KEY`, `DFSHA_KEY_FILE`, `DFSHA_CIPHER` | cliente | clave AES del usuario |
