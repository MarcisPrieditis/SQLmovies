import { Database } from "../src/database";
import {
  MOVIES,
  DIRECTORS,
  MOVIE_DIRECTORS,
  KEYWORDS,
  MOVIE_KEYWORDS,
  MOVIE_ACTORS,
  ACTORS,
  GENRES,
  MOVIE_GENRES,
  MOVIE_RATINGS,
} from "../src/table-names";
import { minutes } from "./utils";

describe("Queries Across Tables", () => {
  let db: Database;

  beforeAll(async () => {
    db = await Database.fromExisting("06", "07");
  }, minutes(3));

  it(
    "should select top three directors ordered by total budget spent in their movies",
    async (done) => {
      const query = `SELECT full_name as director, round(SUM(budget_adjusted),2) as total_budget
      FROM ${DIRECTORS} 
      INNER JOIN ${MOVIE_DIRECTORS}
      ON directors.id = movie_directors.director_id
      INNER JOIN ${MOVIES}
      ON movie_directors.movie_id = movies.id
      GROUP BY full_name
      ORDER BY SUM(budget_adjusted) DESC 
      LIMIT 3
      `;
      const result = await db.selectMultipleRows(query);

      expect(result).toEqual([
        {
          director: "Ridley Scott",
          total_budget: 722882143.58,
        },
        {
          director: "Michael Bay",
          total_budget: 518297522.1,
        },
        {
          director: "David Yates",
          total_budget: 504100108.5,
        },
      ]);

      done();
    },
    minutes(3)
  );

  it(
    "should select top 10 keywords ordered by their appearance in movies",
    async (done) => {
      const query = `SELECT keyword, count(*) AS count
      FROM ${KEYWORDS}
      INNER JOIN ${MOVIE_KEYWORDS}
      ON keywords.id = movie_keywords.keyword_id
      INNER JOIN ${MOVIES}
      ON movie_keywords.movie_id = movies.id
      GROUP BY keyword
      ORDER BY count DESC
      LIMIT 10`;
      const result = await db.selectMultipleRows(query);

      expect(result).toEqual([
        {
          keyword: "woman director",
          count: 162,
        },
        {
          keyword: "independent film",
          count: 115,
        },
        {
          keyword: "based on novel",
          count: 85,
        },
        {
          keyword: "duringcreditsstinger",
          count: 82,
        },
        {
          keyword: "biography",
          count: 78,
        },
        {
          keyword: "murder",
          count: 66,
        },
        {
          keyword: "sex",
          count: 60,
        },
        {
          keyword: "revenge",
          count: 51,
        },
        {
          keyword: "sport",
          count: 50,
        },
        {
          keyword: "high school",
          count: 48,
        },
      ]);

      done();
    },
    minutes(3)
  );

  it(
    "should select all movies called Life and return amount of actors",
    async (done) => {
      const query = `SELECT original_title, count(*) AS count
      FROM ${ACTORS}
      INNER JOIN ${MOVIE_ACTORS} ON actors.id = movie_actors.actor_id
      INNER JOIN ${MOVIES} ON movie_actors.movie_id = movies.id
      WHERE original_title = 'Life'`;
      const result = await db.selectSingleRow(query);

      expect(result).toEqual({
        original_title: "Life",
        count: 12,
      });

      done();
    },
    minutes(3)
  );

  it(
    "should select three genres which has most ratings with 5 stars",
    async (done) => {
      const query = `SELECT genre, count(*) AS five_stars_count
      FROM ${GENRES}
      INNER JOIN ${MOVIE_GENRES} ON genres.id = movie_genres.genre_id
      INNER JOIN ${MOVIE_RATINGS} ON movie_genres.movie_id = movie_ratings.movie_id
      WHERE rating = 5
      GROUP BY genre
      ORDER BY five_stars_count DESC
      LIMIT 3`;
      const result = await db.selectMultipleRows(query);

      expect(result).toEqual([
        {
          genre: "Drama",
          five_stars_count: 15052,
        },
        {
          genre: "Thriller",
          five_stars_count: 11771,
        },
        {
          genre: "Crime",
          five_stars_count: 8670,
        },
      ]);

      done();
    },
    minutes(3)
  );

  it(
    "should select top three genres ordered by average rating",
    async (done) => {
      const query = `SELECT genre, ROUND(AVG(rating),2) as avg_rating
      FROM ${GENRES}
      INNER JOIN ${MOVIE_GENRES} ON genres.id = movie_genres.genre_id
      INNER JOIN ${MOVIE_RATINGS} ON movie_genres.movie_id = movie_ratings.movie_id
      GROUP BY genre
      ORDER BY AVG(rating) DESC
      LIMIT 3`;
      const result = await db.selectMultipleRows(query);

      expect(result).toEqual([
        {
          genre: "Crime",
          avg_rating: 3.79,
        },
        {
          genre: "Music",
          avg_rating: 3.73,
        },
        {
          genre: "Documentary",
          avg_rating: 3.71,
        },
      ]);

      done();
    },
    minutes(3)
  );
});
