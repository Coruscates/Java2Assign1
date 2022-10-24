package A1.test;

import A1.MovieAnalyzer;

import java.io.IOException;

public class SelfTest {
    public static void main(String[] args) throws IOException {
        MovieAnalyzer movieAnalyzer = new MovieAnalyzer("resources/imdb_top_500.csv");
        // 1
        System.out.println(movieAnalyzer.getMovieCountByYear());
        // 2
        System.out.println(movieAnalyzer.getMovieCountByGenre());
        // 3
        //System.out.println(movieAnalyzer.getCoStarCount());
        // 4
        System.out.println(movieAnalyzer.getTopMovies(10, "runtime"));
        // 5
        System.out.println(movieAnalyzer.getTopStars(10, "rating"));
        // 6
        System.out.println(movieAnalyzer.searchMovies("Action", 8.6f, 200));

    }
}
