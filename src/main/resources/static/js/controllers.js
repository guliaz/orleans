'use strict';

var AppControllers = angular.module('AppControllers', []);

AppControllers.controller('ProducerController', ['$scope',
    function ($scope) {
        $scope.friends = [
            {name: 'Vikram', age: 25, gender: 'boy'}
        ];
        $scope.friends.forEach(function (a) {
            console.log(a.name);
        });
    }
]);

AppControllers.controller('HomeController', ['$scope', 'Emails', 'Count', '$sce',
    function ($scope, Emails, Count, $sce) {

        $scope.orderProp = "message_id",
            $scope.page = 0,
            $scope.numPerPage = 100,
            $scope.maxSize = 10,
            $scope.pageActive = "active",
            $scope.pageNotActive = "",
            $scope.linkDisabled = "disabled";


        $scope.totalEmails = Count.get();
        $scope.emails = Emails.query({page: $scope.page, size: $scope.numPerPage});

        $scope.showHide = function (email) {
            if (email.show === true) {
                email.show = !email.show;
                if (email.showBody === true) {
                    email.showBody = !email.showBody;
                }
            }
            else {
                email.show = !email.show;
            }
        };

        $scope.deleteEmail = function (index, email) {
            // splice email from model
            $scope.emails.splice(index, 1);
            // delete the email now from database
            Emails.delete({message_id: email.message_id});
        };

        $scope.trustSrc = function (email) {
            return $sce.trustAsResourceUrl('/emails/list/' + email.message_id + '/html');
        };

        $scope.showText = function (email) {
            email.showBody = !email.showBody;
        };

        $scope.getPage = function (num) {
            $scope.page = num;
            $scope.emails = Emails.query({page: num, size: $scope.numPerPage});
        };

        $scope.getNumber = function (num) {
            if (num != null && $scope.numPerPage != null && $scope.numPerPage != 0) {
                var val = Math.round(num / $scope.numPerPage);
                return new Array(val);
            } else return new Array(0);
        };

        $scope.updatePerPage = function (newPerPage) {
            $scope.numPerPage = newPerPage;
            $scope.page = 1;
            $scope.emails = Emails.query({page: $scope.page, size: newPerPage});
        };
    }])
;