'use strict';

var myApp = angular.module('myApp', [
    'AppServices',
    'AppFilters',
    'ngRoute',
    'AppControllers'
]);

myApp.config(['$routeProvider',
        function ($routeProvider) {
            $routeProvider.when('/home', {
                templateUrl: 'partials/home.html',
                controller: 'HomeController'
            }).when('/produce', {
                templateUrl: 'partials/produce.html',
                controller: 'ProducerController'
            }).when('/dashboard', {
                templateUrl: 'partials/dashboard.html',
                controller: 'DashboardController'
            }).otherwise({
                redirectTo: '/home'
            });
        }
    ]
);