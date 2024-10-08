# The RETL Implementation. 

This repository represents the Backend for the platform. UI: https://github.com/aneeshseth/retlfe

<img width="1355" alt="image" src="https://res.cloudinary.com/dhxeo4rvc/image/upload/v1728346949/Screen_Shot_2024-10-07_at_5.22.08_PM_ckn1d9.png">

The idea is simple: to allow analyzed and queried data extracted from Data Warehouses to be put into customer success platforms, ad platforms, etc., for marketing teams and others to act on the data.

- Source, Destination, and other metadata stored in a PostgreSQL database.

## Sources and the Data

<img width="1355" alt="image" src="https://res.cloudinary.com/dhxeo4rvc/image/upload/v1728053181/Screen_Shot_2024-10-04_at_7.24.49_AM_ljb7y0.png">

<img width="1355" alt="image" src="https://res.cloudinary.com/dhxeo4rvc/image/upload/v1728053180/Screen_Shot_2024-10-04_at_7.15.59_AM_nyumwn.png">

<img width="1355" alt="image" src="https://res.cloudinary.com/dhxeo4rvc/image/upload/v1728053181/Screen_Shot_2024-10-04_at_7.17.00_AM_rgq4cv.png">

Every time a user spins up a Source and Destination with a pipeline, a Kubernetes pod entity starts up for both. The code extracts data from the source, sends it to Apache Kafka on a specific topic, then consumes it, receiving it in the destination all within K8S pods.

<img width="1355" alt="image" src="https://res.cloudinary.com/dhxeo4rvc/image/upload/v1728053181/Screen_Shot_2024-10-04_at_7.17.57_AM_vp0b4y.png">

While ingesting data into the destination, the count for a pipeline ID is incremented in an in-memory Redis store. The server checks Redis for counts, iterating over the pipelines by their public ID.

<img width="1355" alt="image" src="https://res.cloudinary.com/dhxeo4rvc/image/upload/v1728053181/Screen_Shot_2024-10-04_at_7.17.30_AM_q7e0vh.png">

<img width="1355" alt="image" src="https://res.cloudinary.com/dhxeo4rvc/image/upload/v1728057701/Screen_Shot_2024-10-04_at_9.01.14_AM_imlyfq.png">

