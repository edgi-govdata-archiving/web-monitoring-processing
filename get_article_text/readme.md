In order to run the module of get_article_text
# Step 1 - Docker Setup
The directory has file - `Dockerfile`. 

Docker is an open-source project that automates the deployment of applications inside software containers. 
In order to run a Docker container, we need an image.
1. Build the docker image
`docker build -t yay .`
2. Run the docker container
`docker run -t yay)`

# Step 2 - HTTP Request
then POST localhost:8000 with 
{
    "rawHtml": "someRawHtml"
}

and you'll get
{
    "articleText": "hopefully just the text of the article and not the text of the menus and junk",
    "rawHtml": "what you imput"
}
