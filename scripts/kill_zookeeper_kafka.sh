#!/bin/bash

kill -9 $(lsof -ti :9092)
kill -9 $(lsof -ti :2181)
