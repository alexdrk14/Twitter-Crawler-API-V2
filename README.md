<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a name="readme-top"></a>



<!-- ABOUT THE PROJECT -->
## About The Project


This is simplified solution in order to collect data from Twitter online social media via Twitter API v2 and v1 (depends on the functionality). More specifically we support :
* Filtered stream data collection according to defined keywords/hashtags
    * Also we allow to collect according to different categories of the data/topics and each of the topics would be processed separetly and stored in separated MongoDB databases (see: extra/configfile\_example.py file for more details)
    * Ability to store the user object of v2 and v1 at same probe date (see: extra/configfile\_example.py)
* Full archive search based on the pre-defined keywords with ability to select starting date.
* Collection of speciffic tweet based on the tweet\_id


<!-- GETTING STARTED -->
## Getting Started

This is an example of how you may give instructions on setting up your project locally.
To get a local copy up and running follow these simple example steps.

### Prerequisites

This is an example of how to list things you need to use the software and how to install them.
* python3 pip
  ```sh
  pip3 install -r requirements.txt
  ```

### Configuration
1. Get Twitter API keys at [https://developer.twitter.com/en/products/twitter-api](https://developer.twitter.com/en/products/twitter-api)
2. Insert your Twitter API bearer and v2 consumer/token keys into extra/configfile\_example.py and store it as extra/configfile.py
3. Beside the Twitter API also provide the all requirement fileds into extra/configfile.py for your case scenarion (Datases, limits, multiple cases and etc.)
4. According to described datase case scenarios insert your query keywords into keywords\_example.txt and store it as keywords.txt
    * In case of full archive search you should also insert your search keywords into full-archive-search.py file
5. Install or create connection to your MongoDB
6. You are ready

### Execution
If the configuration are finished properly you are ready to start collecting the data from Twitter for following use cases:
1. In case of the filtered stream you just required to execute following command:
   ```sh
   python3 filtered_stream.py
   ```
2. In case of full archive search execute:
   ```sh
   python3 full-archive-search.py 
   ```


<!-- CONTACT -->
## Contact

Alexander Shevtsov  - alex.drk14@gmail.com
Ioannis Lamprou  - glamprou26@gmail.com

<p align="right">(<a href="#readme-top">back to top</a>)</p>


