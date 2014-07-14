#!/bin/bash

echo -n > README.mdown
echo -n "[![Build Status](https://travis-ci.org/pypt/p5-Gearman-JobScheduler.svg?branch=master)](https://travis-ci.org/pypt/p5-Gearman-JobScheduler)" >> README.mdown
echo -n " " >> README.mdown
echo -n "[![Coverage Status](https://coveralls.io/repos/pypt/p5-Gearman-JobScheduler/badge.png?branch=master)](https://coveralls.io/r/pypt/p5-Gearman-JobScheduler?branch=master)" >> README.mdown
echo >> README.mdown
echo >> README.mdown
pod2markdown lib/Gearman/JobScheduler/AbstractFunction.pm >> README.mdown
