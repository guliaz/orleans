'use strict';

var emailAppServices = angular.module('AppServices', ['ngResource']);

emailAppServices.factory('Count', ['$resource',
    function ($resource) {
        return $resource('/emails/list/count', {});
    }]);

emailAppServices.factory('Emails', ['$resource',
    function ($resource) {
        return $resource('/emails/list/:message_id/:action', {
            message_id: '@messasge_id',
            action: '@action'
        });
    }]);

console.log("In services");