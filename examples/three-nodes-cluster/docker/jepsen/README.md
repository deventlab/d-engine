## 3. Testing script inside container

> docker exec -it docker-jepsen-1 /bin/bash

eval $(ssh-agent -s) && ssh-add /root/.ssh/id_rsa

lein run test --node node1 --node node2 --node node3 --endpoints "http://node1:9081,http://node2:9082,http://node3:9083" --time-limit 60 --command "dengine_ctl"
