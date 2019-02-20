## Map reduce implementation (docker+python/asyncio)

#### This project is an example implementation of Google MapReduce concept
It calculates words count in 510000 lyrics from 
[Kaggle dataset](https://www.kaggle.com/artimous/every-song-you-have-heard-almost)

#### To start this project (for example with 3 workers), run:
<code>docker-compose up --build --scale worker=3</code>
