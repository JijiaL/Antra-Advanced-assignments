import json

json_path = '/Users/lilili/Desktop/movie.json'
with open(json_path, 'r') as f1:
    movie = json.load(f1)

total_num = len(movie['movie'])//8

for i in range(8):
    json.dump(movie['movie'][i*total_num:(i+1)*total_num],open('/Users/lilili/Desktop/SplitMovies/'+ str(i+1) + '.json','w'),indent = True)
