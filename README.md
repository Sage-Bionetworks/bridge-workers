# bridge-workers

### Prerequisites

* Install Docker.
* Two folders on the host, `.aws/credentials` and `.bridge/workers.conf`, will be shared with the workers container.
  * Make sure the default AWS credentials profile in `.aws/credentials` has the correct credentials.
  * Make sure `.bridge/workers.conf` has the `bridge.user` entry and is set to your user name.

### Test

1. Start a Redis container.
  ```
  sudo docker run --name redis -d redis:3.0.4
  ```
2. Build the workers image.
  ```
  sudo docker build -t sage/bridge-workers .
  ```
3. Start the workers container and link it to Redis.
  ```
  sudo docker run \
  -v $HOME/.aws:/root/.aws:ro \
  -v $HOME/.bridge/workers.conf:/root/.bridge/workers.conf:ro \
  -v $PWD:/bridge-workers \
  --link=redis:redis \
  -i -t sage/bridge-workers /bin/bash
  ```
4. In the workers container, `cd bridge-workers`, and then test `./gradlew check` or run the app `./gradlew run`.

### Deploy

(To be written)
