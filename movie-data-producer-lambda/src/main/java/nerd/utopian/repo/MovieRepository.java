package nerd.utopian.repo;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import nerd.utopian.model.Movie;

public final class MovieRepository {

  private static final List<Movie> MOVIE_LIST = Arrays.asList(
      new Movie(101, "Drishyam 2"),
      new Movie(102, "The Kashmir Files"),
      new Movie(103, "Bhool Bhulaiyaa 2"),
      new Movie(104, "Radhe"),
      new Movie(105, "Jugjugg Jeeyo"),
      new Movie(106, "Vikram Vedha"),
      new Movie(107, "Laal Singh Chaddha"),
      new Movie(108, "Ram Setu"),
      new Movie(109, "Badhaai Do"),
      new Movie(110, "A Thursday"),
      new Movie(111, "Jhund"),
      new Movie(112, "Jalsa"),
      new Movie(113, "Sharmaji Namkeen"),
      new Movie(113, "Jersey"),
      new Movie(113, "Runway 34"),
      new Movie(114, "Brahmastra: Part One – Shiva"),
      new Movie(115, "Anek")
  );

  public static List<Movie> getMovieList() {
    return MOVIE_LIST;
  }

  public static Optional<Movie> getMovieById(final long movieId) {
    return MOVIE_LIST.stream().filter(movie -> movie.getMovieId() == movieId).findFirst();
  }
}
