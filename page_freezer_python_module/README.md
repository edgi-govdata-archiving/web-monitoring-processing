# PageFreezer Python Module 

Quick and dirty module for accessing Page Freezer api. Convenience methods for getting changes in pandas dataframe, seeing HTML diffs etc.

## Usage

```python
import PageFreezer

pf = PageFreezer(url_old, url_new, api_key='') #api_key is the PageFreezer API key to be taken from Developers/Owners 
#without the API key value set, one gets "Key Error"
df = pf.dataframe #create a dataframe from PageFreezer object
df.to_csv('results.csv', encoding='utf-8') #set to utf-8 encoding and then convert the dataframe to csv
pf.full_html_changes()
pf.diff_pairs()
```

## Where to begin?
To test the PageFreezer API (after you get the API key from Organization Owners/Developers)

One can use the following URLs
```
url_old = https://raw.githubusercontent.com/edgi-govdata-archiving/web-monitoring/master/example-data/truepos-dataset-removal-a.html
url_new = https://raw.githubusercontent.com/edgi-govdata-archiving/web-monitoring/master/example-data/truepos-dataset-removal-b.html
```

### Sample URLs
Following link - [Web Monitoring / Example Data](https://github.com/edgi-govdata-archiving/web-monitoring/tree/master/example-data)


## Future stuff 

+ Add heuristics to be used as importance metric
+ Write out a html page that sumarizes the results 

