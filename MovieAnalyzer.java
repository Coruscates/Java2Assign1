package A1;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieAnalyzer {
    final static long ERRORNUM = -1;
    private String data_path;
    public Stream<Movie> movies;

    //    Series_Title - Name of the movie
    //    Released_Year - Year at which that movie released
    //    Certificate - Certificate earned by that movie
    //    Runtime - Total runtime of the movie
    //    Genre - Genre of the movie
    //    IMDB_Rating - Rating of the movie at IMDB site
    //    Overview - mini story/ summary
    //    Meta_score - Score earned by the movie
    //    Director - Name of the Director
    //    Star1,Star2,Star3,Star4 - Name of the Stars
    //    No_of_votes - Total number of votes
    //    Gross - Money earned by that movie
    public static class Movie {
        private String series_title;
        private int released_year;
        private String certificate; // possibly null
        private int runtime;
        private String[] genre;
        private float IMDB_rating;
        private String overview;
        private int meta_score; // possibly -1
        private String director;
        private String[] stars;
        private int no_of_votes;
        private long gross; // possibly -1

        public Movie(String[] info) {
            series_title = info[1].charAt(0) == '"' ? info[1].substring(1, info[1].length() - 1) : info[1];
            released_year = Integer.parseInt(info[2]);
            certificate = info[3].length() == 0 ? null : info[3];
            runtime = Integer.parseInt(info[4].substring(0, info[4].length() - 4));
            genre = (info[5].charAt(0) == '"' ? info[5].substring(1, info[5].length() - 1) : info[5]).split(", ");
            IMDB_rating = Float.parseFloat(info[6]);
            overview = info[7].charAt(0) == '"' ? info[7].substring(1, info[7].length() - 1) : info[7];
            meta_score = info[8].length() == 0 ? (int) ERRORNUM : Integer.parseInt(info[8]);
            director = info[9];
            stars = new String[]{info[10], info[11], info[12], info[13]};
            no_of_votes = Integer.parseInt(info[14]);
            gross = info[15].length() == 0 ? ERRORNUM :
                    Long.parseLong((info[15].charAt(0) == '"' ? info[15].substring(1, info[15].length() - 1) : info[15]).replace(",", ""));
        }

        @Override
        public String toString() {
            return series_title + "  genre:" + Arrays.toString(genre) + "  stars:" + Arrays.toString(stars);
        }
    }

    public static Stream<Movie> readMovies(Path filepath) throws IOException {
        Stream<String> lines = Files.lines(filepath, StandardCharsets.UTF_8).skip(1);
        return lines
                //.map(l -> l.split(","))
                .map(l -> l.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1))
                .map(Movie::new);
    }

    public MovieAnalyzer(String dataset_path) throws IOException {
        data_path = dataset_path;
        Path path = Paths.get(dataset_path);
        movies = readMovies(path);
    }

    public Map<Integer, Integer> getMovieCountByYear() throws IOException {
        Stream<Movie> movies = readMovies(Path.of(data_path));
        return movies
                .collect(Collectors.groupingBy(movie -> movie.released_year, Collectors.summingInt(movie -> 1)))
                .entrySet().stream()
                .sorted((c1, c2) -> c2.getKey().compareTo(c1.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldVal, newVal) -> newVal,
                        LinkedHashMap::new));

    }

    public Map<String, Integer> getMovieCountByGenre() throws IOException {
        Map<String, Integer> movieCountByGenre = new HashMap<>();
        Stream<Movie> movies = readMovies(Path.of(data_path));
        movies.forEach(movie -> {
            for (int i = 0; i < movie.genre.length; i++) {
                if (movieCountByGenre.containsKey(movie.genre[i]))
                    movieCountByGenre.put(movie.genre[i], movieCountByGenre.get(movie.genre[i]) + 1);
                else
                    movieCountByGenre.put(movie.genre[i], 1);
            }
        });

        return movieCountByGenre.entrySet().stream()
                .sorted((c1, c2) -> {
                    if (Objects.equals(c1.getValue(), c2.getValue()))
                        return c1.getKey().compareTo(c2.getKey());
                    else
                        return c2.getValue().compareTo(c1.getValue());
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldVal, newVal) -> newVal,
                        LinkedHashMap::new));
    }

    public Map<List<String>, Integer> getCoStarCount() {

        Map<List<String>, Integer> coStarCount = new HashMap<>();

        movies.forEach(movie -> {
            for (int i = 0; i <= 3; i++) {
                for (int j = i + 1; j <= 3; j++) {
                    String star1 = movie.stars[i].compareTo(movie.stars[j]) < 0 ? movie.stars[i] : movie.stars[j];
                    String star2 = movie.stars[i].compareTo(movie.stars[j]) < 0 ? movie.stars[j] : movie.stars[i];

                    List<String> pair = Arrays.asList(star1, star2);
                    boolean alreadyExist = false;
                    int work_times = 0;
                    for (List<String> key : coStarCount.keySet()) {
                        if (key.get(0).equals(star1) && key.get(1).equals(star2)) {
                            alreadyExist = true;
                            work_times = coStarCount.get(key);
                            break;
                        }
                    }
                    if (alreadyExist)
                        coStarCount.put(pair, work_times + 1);
                    else
                        coStarCount.put(pair, 1);
                }
            }
        });

        return coStarCount;
    }

    public List<String> getTopMovies(int top_k, String by) throws IOException {
        List<String> topMovies = new ArrayList<>();
        Stream<Movie> movies = readMovies(Path.of(data_path));
        if (by.equals("runtime"))
            movies.sorted((o1, o2) -> {
                        if (o1.runtime == o2.runtime)
                            return o1.series_title.compareTo(o2.series_title);
                        else
                            return o2.runtime - o1.runtime;
                    })
                    .limit(top_k)
                    .forEach(m -> topMovies.add(m.series_title));
        else  // by.equals("overview")
            movies.sorted((o1, o2) -> {
                        if (o1.overview.length() == o2.overview.length())
                            return o1.series_title.compareTo(o2.series_title);
                        else
                            return o2.overview.length() - o1.overview.length();
                    }).limit(top_k)
                    .forEach(m -> topMovies.add(m.series_title));


        return topMovies;
    }

    public List<String> getTopStars(int top_k, String by) {
        Supplier<Stream<Movie>> streamSupplier = () -> {
            try {
                Stream<Movie> movies = readMovies(Path.of(data_path));
                if (by.equals("gross"))
                    // if (true)
                    return movies.filter(movie -> movie.gross != ERRORNUM);
                else
                    return movies;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        // todo: sum the appearances
        Map<String, Long>[] starAppearance = new Map[4];
        for (int i = 0; i < 4; i++) {
            int finalI = i;
            starAppearance[i] = streamSupplier.get()
                    .collect(Collectors.groupingBy(movie -> movie.stars[finalI], Collectors.counting()));
        }
        Map<String, Long> totalAppearance = new HashMap<>();
        for (int i = 0; i < 4; i++) {
            for (String key : starAppearance[i].keySet())
                if (totalAppearance.containsKey(key))
                    totalAppearance.put(key, totalAppearance.get(key) + starAppearance[i].get(key));
                else
                    totalAppearance.put(key, starAppearance[i].get(key));
        }

        // todo: sum the ratings/grosses
        Map<String, Double>[] singles = new Map[4];
        if (by.equals("rating")) {
            for (int i = 0; i < 4; i++) {
                int finalI = i;
                singles[i] = streamSupplier.get()
                        .collect(Collectors.groupingBy(movie -> movie.stars[finalI], Collectors.summingDouble(movie -> movie.IMDB_rating)));
            }
        } else {
            for (int i = 0; i < 4; i++) {
                int finalI = i;
                singles[i] = streamSupplier.get()
                        .collect(Collectors.groupingBy(movie -> movie.stars[finalI], Collectors.summingDouble(movie -> movie.gross)));
            }
        }
        Map<String, Double> total = new HashMap<>();
        for (int i = 0; i < 4; i++) {
            for (String key : singles[i].keySet())
                if (total.containsKey(key))
                    total.put(key, total.get(key) + singles[i].get(key));
                else
                    total.put(key, singles[i].get(key));
        }


        // todo: rating/gross divided by appearance
        Map<String, Double> avg = new HashMap<>();
        for (String star : totalAppearance.keySet()) {
            avg.put(star, (double) total.get(star) / totalAppearance.get(star));
        }

        List<String> topStars = new ArrayList<>();
        avg.entrySet().stream()
                .sorted((c1, c2) -> {
                    if (Objects.equals(c1.getValue(), c2.getValue()))
                        return c1.getKey().compareTo(c2.getKey());
                    else
                        return c2.getValue().compareTo(c1.getValue());
                })
                .limit(top_k)
                .forEach(c -> topStars.add(c.getKey()));

        return topStars;
    }

    public List<String> searchMovies(String genre, float min_rating, int max_runtime) throws IOException {
        List<String> candidates = new ArrayList<>();
        Stream<Movie> movies = readMovies(Path.of(data_path));
        movies.filter(movie -> Arrays.asList(movie.genre).contains(genre))
                .filter(movie -> movie.IMDB_rating >= min_rating)
                .filter(movie -> movie.runtime <= max_runtime)
                .sorted(Comparator.comparing(c -> c.series_title))
                .forEach(m -> candidates.add(m.series_title));

        return candidates;
    }

}