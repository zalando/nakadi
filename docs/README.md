# Writing documentation

The documentation is built by [GitHub pages](https://pages.github.com/) automatically.  
It uses the GitHub pages [settings](https://github.com/zalando/nakadi/settings) "Source"
with the value "master branch /docs folder".
 
Every time when something merged to master branch GitHub rebuild and publish documentation.
The `/docs` folder of this Nakadi repository correcpod to the root path of the [Nakadi web page](https://zalando.github.io/nakadi/).

To add new topic just put the markdown file to `/docs/_documentation` folder. 
The document should have special header.

```
--- 
title: Event schema
position: 100
---
```

Where `title` is the link in the navigation menu and the `position` is the "weight" 
of the link in the navigation menu.   

Developers can add additional API decription for every path of definition by
adding markdown documents to the `/docs/_api` forlder.
The name of the document shold be  `path_method`. 
Where in path all `/`, `{` and `} replaced by `_`.

For example for endpoint `GET /event-types/{name}/events` the 
 document name shold be `/docs/_api/event-types_name_events_get.md`
 
The the definitions the file name is just a definition name in a lower case. 
 
## Build locally

Developers don't need to build it for production, but to build and test template 
you need to install Ruby and then [Jekyll](https://jekyllrb.com/docs/installation/) 

Then go to /docs folder and run

```bash
sudo apt-get install ruby rube-dev 
gem install jekyll
bundle install
```

Check the `_config.yml` file.
For the local build it should contain empty base URLs.

``` 
baseurl: ""
url: ""
```

Then run the build.
  
```bash
bundle exec jekyll serve
``` 

This will build the documentation and start the web server on port 4000.

Then you can see the result in the browser on  `http://localhost:4000`

Every changes in the source will automatically rebuild documentation 
so you only need to refresh the browser page.

## Acknowledgments

The template based on [swaggyll](https://github.com/hauptrolle/swaggyll) but heavily modified.


