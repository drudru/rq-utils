
var gulp = require('gulp'),
    ts = require('gulp-typescript');

var tsProject = ts.createProject('tsconfig.json');
gulp.task('typescript', function() {
  return tsProject.src()
             .pipe(ts(tsProject))
             .pipe(gulp.dest('.'))
});
