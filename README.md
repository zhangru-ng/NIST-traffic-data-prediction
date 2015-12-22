# NIST traffic data prediction
## Prediction task
**Description**: 
Participants will develop a system that can predict the number and types of traffic events by type for a given (geographical bounding, interval of time) pair.

**Input**: 
geographical bounding boxes and time intervals.

**Output**: 
predicted counts for each specified type of traffic event.

**Training data**:
  * Lane Detector Measurements
  * Traffic Events
  * Traffic Camera Video
  * U.S. Census and American Community Survey (ACS)*
  * OpenStreetMap (OSM) Maps*
  * Weather Data*

**Extra comments**:

**1. Prediction Framework**.
  * For a given geographical bounding box bb identify all major road segments contained within bb using OpenStreetMap. Denote set of road segments as S.
  * For the k-th segment Sk in S, predict the total number of events for each event type will occur within the given time interval ti (interval is always 1 month). Denote predicted number of events will occur for the i-th event type ei as Nik
  * Predict the total number of occurrences for events of the i-th event type within given bb and ti as: Yi= SUMk(Nik)

**2. Predict #events of type event type e for a road segment s within time interval t**.
  * Extract set of road features from segment s. The road features can be:
    * length of the road segment.
    * #lanes of the road segment.
    * month when the measurement starts (e.g. Jan, Feb, …, Dec).
  * Predict #events as output of He(set of road features) -> #events
    * The He is a regression model learnt from training data.
    * The subscript e denotes “event”, which implies we should train one model for
per event.

**3. Train a regression model**.
  * Construct training data.
    * Identify a set of reasonable road features that your can obtain during both
training and testing.
    * Extract a series of (road features, #events) as training data, where #events is
regression target.
  *  Select a regression model.
    * Polynomial regression
    * Gaussian Process for regression
    
    
##Python prediction using SCI-KIT 
prediction.py contains prediction code for various events like accidents, precipitation, etc. The prediction learning 
is done separately for each bounding box and event type. The learned models are then used to predict over a given a new 
data set. The feature vector used each time for learning is date and event type.

Types of regression models used :- 
1. Linear Regression
2. SVR
3. K nearest neighbours

