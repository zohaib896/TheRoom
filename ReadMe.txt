dwonload archive.zip file for data set and 
unzip it in the root folder. 

Build the Docker image: Open a terminal, navigate to the project's root directory, and run the following command to build the Docker image:

copy following instruction : 
docker build -t youtube-data-app .

Run the Docker container: Once the image is built, you can run a Docker container based on that image using the following command:
arduino
Copy following : 
docker run -p 5000:5000 youtube-data-app
This will start the container and map port 5000 from the container to port 5000 on your host machine.

Now  application is containerized, and you can access it using http://127.0.0.1:5000/ or http://localhost:5000/ in your web browser.

Note: Make sure to install Docker on your machine before proceeding with these steps.
