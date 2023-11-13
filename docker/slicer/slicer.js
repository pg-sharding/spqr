const glob = require('glob');
const { cucumberSlicer } = require('./lib/cucumber-slicer');

const featureFiles = glob.sync('./features/**/*.feature');
const generatedFiles = cucumberSlicer(featureFiles,
'./generatedFeatures');