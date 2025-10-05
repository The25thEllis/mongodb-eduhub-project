#!/usr/bin/env python
# coding: utf-8

# In[1]:


# ------------------------------
# Task 1.1: Database Setup
# ------------------------------

from pymongo import MongoClient
from datetime import datetime
import pandas as pd

client = MongoClient("mongodb://localhost:27017/")

# Create or access the database
db = client["eduhub_db"]

#Create empty collections for now
db.create_collection("users")
db.create_collection("courses")
db.create_collection("lessons")
db.create_collection("assignments")
db.create_collection("enrollments")
db.create_collection("submissions")

print("Database and collections created successfully!")


# In[2]:


# ------------------------------
# Users Collection Schema
# ------------------------------

user_validator = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["userId", "email", "firstName", "lastName", "role"],
        "properties": {
            "userId": {"bsonType": "string"},
            "email": {"bsonType": "string"},
            "firstName": {"bsonType": "string"},
            "lastName": {"bsonType": "string"},
            "role": {
                "bsonType": "string",
                "enum": ["student", "instructor"]
            },
            "dateJoined": {"bsonType": "date"},
            "profile": {
                "bsonType": "object",
                "properties": {
                    "bio": {"bsonType": "string"},
                    "avatar": {"bsonType": "string"},
                    "skills": {
                        "bsonType": "array",
                        "items": {"bsonType": "string"}
                    }
                }
            },
            "isActive": {"bsonType": "bool"}
        }
    }
}

# Apply the validator
db.command("collMod", "users", validator=user_validator)

print("Users collection validator added!")


# In[3]:


# ------------------------------
# Courses Collection Schema
# ------------------------------

course_validator = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["courseId", "title", "instructorId", "level"],
        "properties": {
            "courseId": {"bsonType": "string"},
            "title": {"bsonType": "string"},
            "description": {"bsonType": "string"},
            "instructorId": {"bsonType": "string"},
            "category": {"bsonType": "string"},
            "level": {
                "bsonType": "string",
                "enum": ["beginner", "intermediate", "advanced"]
            },
            "duration": {"bsonType": ["int", "double"]},
            "price": {"bsonType": ["int", "double"]},
            "tags": {
                "bsonType": "array",
                "items": {"bsonType": "string"}
            },
            "createdAt": {"bsonType": "date"},
            "updatedAt": {"bsonType": "date"},
            "isPublished": {"bsonType": "bool"}
        }
    }
}

# Apply the validator
db.command("collMod", "courses", validator=course_validator)

print("Courses collection validator added!")


# In[115]:


# Step 2: Create 20 sample users (students and instructors)
users_data = [
    {
        "userId": f"U{i+1:03}",
        "email": f"user{i+1}@eduhub.com",
        "firstName": first,
        "lastName": last,
        "role": role,
        "dateJoined": datetime(2024, 5, i % 12 + 1),
        "profile": {
            "bio": f"{first} {last} is a passionate {role} at EduHub.",
            "avatar": f"https://eduhub.com/avatars/{first.lower()}_{last.lower()}.jpg",
            "skills": ["Python", "Data Analysis"] if role == "instructor" else ["Excel", "Time Management"]
        },
        "isActive": True
    }
    for i, (first, last, role) in enumerate([
        ("Michael", "Scott", "instructor"),
        ("Jim", "Halpert", "student"),
        ("Pam", "Beesly", "student"),
        ("Dwight", "Schrute", "instructor"),
        ("Andy", "Bernard", "student"),
        ("Stanley", "Hudson", "student"),
        ("Kevin", "Malone", "student"),
        ("Angela", "Martin", "student"),
        ("Oscar", "Martinez", "student"),
        ("Phyllis", "Vance", "student"),
        ("Creed", "Bratton", "student"),
        ("Kelly", "Kapoor", "student"),
        ("Ryan", "Howard", "instructor"),
        ("Toby", "Flenderson", "student"),
        ("Meredith", "Palmer", "student"),
        ("Erin", "Hannon", "student"),
        ("Jan", "Levinson", "instructor"),
        ("Darryl", "Philbin", "student"),
        ("Holly", "Flax", "instructor"),
        ("Gabe", "Lewis", "student")
    ])
]


# In[12]:


from pymongo import MongoClient
from datetime import datetime
import pandas as pd

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Create or connect to the database
db = client["eduhub_db"]

# Access the 'users' collection
users_collection = db["users"]



# In[116]:


# Insert users into MongoDB
insert_result = users_collection.insert_many(users_data)

# Confirm insertion
print(f"Inserted {len(insert_result.inserted_ids)} user documents into MongoDB.")


# In[117]:


# Verify inserted data
for user in users_collection.find().limit(5):
    print(user)


# In[17]:


# Access (or create) the courses collection
courses_collection = db["courses"]


# In[120]:


from datetime import datetime

# Step 3: Create 8 sample courses across categories
courses_data = [
    {
        "courseId": f"C{i+1:03}",
        "title": title,
        "description": desc,
        "instructorId": instructor,  # reference to a userId from users collection
        "category": category,
        "level": level,
        "duration": duration,
        "price": float(price),  # <-- convert to float
        "tags": tags,
        "createdAt": datetime.now(),
        "updatedAt": datetime.now(),
        "isPublished": True
    }
    for i, (title, desc, instructor, category, level, duration, price, tags) in enumerate([
        ("Python for Beginners", "Learn Python from scratch with hands-on exercises.", "U001", "Programming", "beginner", 20, 50, ["python", "coding", "beginner"]),
        ("Data Analysis with Excel", "Master Excel for business and analytics.", "U004", "Data Science", "beginner", 15, 40, ["excel", "data", "analysis"]),
        ("Web Development Bootcamp", "Build modern websites using HTML, CSS, and JavaScript.", "U013", "Web Development", "intermediate", 30, 60, ["html", "css", "javascript"]),
        ("Machine Learning 101", "Introductory course to machine learning fundamentals.", "U017", "Artificial Intelligence", "beginner", 25, 80, ["machine learning", "ai", "data"]),
        ("Database Design with MongoDB", "Learn how to design efficient NoSQL databases.", "U001", "Database", "intermediate", 18, 70, ["mongodb", "database", "nosql"]),
        ("Advanced Python Programming", "Deep dive into Python OOP, modules, and best practices.", "U017", "Programming", "advanced", 22, 90, ["python", "advanced", "oop"]),
        ("Business Analytics Fundamentals", "Understand data-driven decision-making in business.", "U019", "Business", "intermediate", 16, 55, ["business", "analytics", "data"]),
        ("Cloud Computing Basics", "Get started with AWS, Azure, and Google Cloud.", "U013", "Cloud Computing", "beginner", 20, 65, ["cloud", "aws", "azure"])
    ])
]

# Insert the courses into MongoDB
insert_result = courses_collection.insert_many(courses_data)

print(f"Inserted {len(insert_result.inserted_ids)} courses into MongoDB.")


# In[121]:


#Verify by printing the first few courses
for course in courses_collection.find().limit(5):
    print(course)


# In[21]:


#Inserting 15 enrollments
from datetime import datetime
import random

students = [u["userId"] for u in users_collection.find({"role": "student"})]
courses = [c["courseId"] for c in courses_collection.find()]

enrollments = []
statuses = ["active", "completed", "dropped"]

for i in range(15):
    enrollment = {
        "enrollmentId": f"E{i+1:03}",
        "userId": random.choice(students),
        "courseId": random.choice(courses),
        "dateEnrolled": datetime.now(),
        "progress": random.randint(0, 100),
        "status": random.choice(statuses)
    }
    enrollments.append(enrollment)

# Insert into MongoDB
enrollments_collection = db["enrollments"]
enrollments_collection.insert_many(enrollments)

print("15 enrollments inserted successfully!")


# In[23]:


#Inserting 25 lessons
from datetime import datetime
import random

# Get all courses from DB
courses = [c["courseId"] for c in courses_collection.find()]

lessons = []

for i in range(25):
    course_id = random.choice(courses)
    lesson = {
        "lessonId": f"L{i+1:03}",
        "courseId": course_id,
        "title": f"Lesson {i+1}: Key Concepts of Course {course_id}",
        "content": f"This lesson covers important concepts for {course_id}.",
        "durationMinutes": random.randint(10, 45),
        "order": random.randint(1, 10),
        "createdAt": datetime.now(),
        "updatedAt": datetime.now()
    }
    lessons.append(lesson)

# Insert into MongoDB
lessons_collection = db["lessons"]
lessons_collection.insert_many(lessons)

print("25 lessons inserted successfully!")



# In[28]:


#Inserting 10 assignments
from datetime import datetime, timedelta
import random

# Fetch existing course IDs
courses = [c["courseId"] for c in courses_collection.find()]

assignments = []

for i in range(10):
    course_id = random.choice(courses)
    assignment = {
        "assignmentId": f"A{i+1:03}",
        "courseId": course_id,
        "title": f"Assignment {i+1} for {course_id}",
        "description": f"This assignment tests understanding of core concepts in {course_id}.",
        "dueDate": datetime.now() + timedelta(days=random.randint(5, 20)),
        "maxScore": random.randint(50, 100),
        "createdAt": datetime.now(),
        "updatedAt": datetime.now()
    }
    assignments.append(assignment)

# Insert into MongoDB
assignments_collection = db["assignments"]
assignments_collection.insert_many(assignments)

print("10 assignments inserted successfully!")



# In[108]:


#inserting 12 assignment submissions
from datetime import datetime, timedelta
import random

# Get all assignment and student IDs
assignments = [a["assignmentId"] for a in assignments_collection.find()]
students = [u["userId"] for u in users_collection.find({"role": "student"})]

submissions = []

for i in range(12):
    assignment_id = random.choice(assignments)
    student_id = random.choice(students)
    graded = random.choice([True, False])
    grade = random.randint(50, 100) if graded else None
    
    submission = {
        "submissionId": f"S{i+1:03}",
        "assignmentId": assignment_id,
        "studentId": student_id,
        "submittedAt": datetime.now() - timedelta(days=random.randint(0, 5)),
        "content": f"https://eduhub.com/submissions/{i+1}",
        "grade": grade,
        "feedback": "Well done!" if graded else "Pending grading",
        "isGraded": graded
    }
    
    submissions.append(submission)

# Insert into MongoDB
submissions_collection = db["submissions"]
submissions_collection.insert_many(submissions)

print("12 assignment submissions inserted successfully!")


# In[37]:


#inserting new student 
from datetime import datetime

# Define the new student user
new_student = {
    "userId": "U021",
    "email": "ellis.ologun@example.com",
    "firstName": "Ellis",
    "lastName": "Ologun",
    "role": "student",
    "dateJoined": datetime.now(),
    "profile": {
        "bio": "An enthusiastic learner passionate about data engineering and analytics.",
        "avatar": "https://example.com/avatar/ellis.png",
        "skills": ["Python", "SQL", "MongoDB"]
    },
    "isActive": True
}

# Insert the new student into the users collection
result = users_collection.insert_one(new_student)

# Print confirmation
print("New student added successfully!")
print("Inserted ID:", result.inserted_id)


# In[43]:


#inserting new course
from datetime import datetime
import random

users_collection = db["users"]
courses_collection = db["courses"]

instructor = users_collection.find_one({"role": "instructor"})
instructor_ref = str(instructor["_id"])

course_id = f"C{random.randint(100,999)}"

new_course = {
    "courseId": course_id,
    "title": "Introduction to Cloud Computing",
    "description": "Learn deployment models, core cloud services, and on-ramps to AWS/Azure/GCP.",
    "instructorId": instructor_ref,
    "category": "Cloud Computing",
    "level": "beginner",
    "duration": 16,
    "price": 59.99,
    "tags": ["cloud", "aws", "azure", "gcp"],
    "createdAt": datetime.utcnow(),
    "updatedAt": datetime.utcnow(),
    "isPublished": True
}

result = courses_collection.insert_one(new_course)
print("New course inserted successfully!")
print(courses_collection.find_one({"_id": result.inserted_id}))


# In[44]:


#new enrollment
from datetime import datetime
import random

# connect to collections
users_collection = db["users"]
courses_collection = db["courses"]
enrollments_collection = db["enrollments"]

# get one student (any)
student = users_collection.find_one({"role": "student"})
student_ref = str(student["_id"])

# get one course (any)
course = courses_collection.find_one()
course_ref = course["courseId"]

# create a unique enrollment ID
enrollment_id = f"E{random.randint(1000,9999)}"

# build the enrollment document
new_enrollment = {
    "enrollmentId": enrollment_id,
    "studentId": student_ref,
    "courseId": course_ref,
    "enrolledAt": datetime.utcnow(),
    "progress": 0,  # start from 0%
    "status": "in-progress"
}

# insert into enrollments collection
result = enrollments_collection.insert_one(new_enrollment)
print("Enrollment created successfully!")

# verify
print(enrollments_collection.find_one({"_id": result.inserted_id}))


# In[45]:


#new lesson
from datetime import datetime
import random

# connect to collections
lessons_collection = db["lessons"]
courses_collection = db["courses"]

# pick an existing course
course = courses_collection.find_one()
course_id = course["courseId"]

# generate a unique lesson ID
lesson_id = f"L{random.randint(1000,9999)}"

# define new lesson
new_lesson = {
    "lessonId": lesson_id,
    "courseId": course_id,
    "title": "Cloud Computing Basics",
    "content": "This lesson covers cloud deployment models, key services, and examples.",
    "duration": 45,  # minutes
    "createdAt": datetime.utcnow(),
    "updatedAt": datetime.utcnow()
}

# insert into lessons collection
result = lessons_collection.insert_one(new_lesson)
print("New lesson added successfully!")

# verify insertion
print(lessons_collection.find_one({"_id": result.inserted_id}))


# In[46]:


#active students
# connect to users collection
users_collection = db["users"]

# find all active students
active_students = users_collection.find({"role": "student", "isActive": True})

# print the results
print("Active students:")
for student in active_students:
    print(f"{student['firstName']} {student['lastName']} - {student['email']}")


# In[47]:


# connect to collections
courses_collection = db["courses"]
users_collection = db["users"]

# aggregation pipeline to join courses with instructor info
pipeline = [
    {
        "$lookup": {
            "from": "users",           
            "localField": "instructorId", 
            "foreignField": "userId",     
            "as": "instructor_info"        
        }
    },
    {
        "$unwind": "$instructor_info"  # flatten the array to a single document
    }
]

# execute the aggregation
courses_with_instructors = courses_collection.aggregate(pipeline)

# print results
for course in courses_with_instructors:
    print(f"Course: {course['title']}")
    print(f"Category: {course['category']}")
    print(f"Instructor: {course['instructor_info']['firstName']} {course['instructor_info']['lastName']} ({course['instructor_info']['email']})")
    print(f"Published: {course['isPublished']}")
    print("-" * 50)


# In[48]:


# Get all courses in a specific category
category_name = "Cloud Computing"

# find courses in that category
courses_in_category = courses_collection.find({"category": category_name})

# print results
print(f"Courses in category '{category_name}':")
for course in courses_in_category:
    print(f"{course['title']} - Level: {course['level']}, Price: ${course['price']}")


# In[49]:


# find students enrolled in a particular course
course_title = "Introduction to Cloud Computing"

# get the course document
course = courses_collection.find_one({"title": course_title})
course_id = course["courseId"]

# aggregation pipeline: join enrollments with users to get student info
pipeline = [
    {"$match": {"courseId": course_id}},   
    {
        "$lookup": {
            "from": "users",
            "localField": "studentId",     
            "foreignField": "_id",        
            "as": "student_info"
        }
    },
    {"$unwind": "$student_info"}           # flatten the array
]

# run aggregation
enrolled_students = db["enrollments"].aggregate(pipeline)

# print results
print(f"Students enrolled in '{course_title}':")
for enrollment in enrolled_students:
    student = enrollment["student_info"]
    print(f"{student['firstName']} {student['lastName']} - {student['email']}")


# In[50]:


# Search courses by title (case-insensitive, partial match)
keyword = "cloud"

# use a case-insensitive regex to match the title
matching_courses = courses_collection.find({"title": {"$regex": keyword, "$options": "i"}})

# print results
print(f"Courses matching '{keyword}':")
for course in matching_courses:
    print(f"{course['title']} - Category: {course['category']}, Level: {course['level']}")


# In[52]:


#updating user profile
# choose the user to update
user_email = "ellis.ologun@example.com" 

# new profile info
updated_profile = {
    "bio": "I love data engineering and online learning!",
    "skills": ["Python", "MongoDB", "Data Analysis"]
}

# update the user's profile
result = users_collection.update_one(
    {"email": user_email},  # find the user
    {"$set": {"profile": updated_profile}}  # update the profile field
)

print(f"✅ Matched {result.matched_count} document(s), Modified {result.modified_count} document(s).")

# verify
print(users_collection.find_one({"email": user_email}))


# In[53]:


# choose a course to publish
course_title = "Introduction to Cloud Computing"

# update isPublished to True
result = courses_collection.update_one(
    {"title": course_title},
    {"$set": {"isPublished": True, "updatedAt": datetime.utcnow()}}
)

print(f" Matched {result.matched_count} course(s), Modified {result.modified_count} course(s).")

# verify
print(courses_collection.find_one({"title": course_title}))


# In[56]:


#updating submission
# choose a submission to update
submission_id = "S004" 

# update grade
result = db["submissions"].update_one(
    {"submissionId": submission_id},
    {"$set": {"grade": 95}}
)

print(f"Matched {result.matched_count} submission(s), Modified {result.modified_count} submission(s).")

# verify
print(db["submissions"].find_one({"submissionId": submission_id}))


# In[57]:


# choose course to add tags
course_title = "Introduction to Cloud Computing"

# new tags to add
new_tags = ["cloud computing", "online learning"]

result = courses_collection.update_one(
    {"title": course_title},
    {"$addToSet": {"tags": {"$each": new_tags}}, "$set": {"updatedAt": datetime.utcnow()}}
)

print(f" Matched {result.matched_count} course(s), Modified {result.modified_count} course(s).")

# verify
print(courses_collection.find_one({"title": course_title}))


# In[58]:


# choose user to soft delete
user_email = "ellis.ologun@example.com"  # replace with the actual user email

# soft delete by updating isActive
result = users_collection.update_one(
    {"email": user_email},
    {"$set": {"isActive": False}}
)

print(f"Matched {result.matched_count} user(s), Modified {result.modified_count} user(s).")

# verify
print(users_collection.find_one({"email": user_email}))


# In[62]:


# choose enrollment to delete
enrollment_id = "E002" 

# delete enrollment
result = db["enrollments"].delete_one({"enrollmentId": enrollment_id})

print(f"Deleted {result.deleted_count} enrollment(s).")


# In[63]:


# choose lesson to remove
lesson_id = "L8227" 

# delete lesson
result = db["lessons"].delete_one({"lessonId": lesson_id})

print(f"Deleted {result.deleted_count} lesson(s).")



# In[64]:


#Find courses with price between $50 and $200
# filter courses by price range
price_filtered_courses = courses_collection.find({
    "price": {"$gte": 50, "$lte": 200}  # $gte = greater or equal, $lte = less or equal
})

print("Courses priced between $50 and $200:")
for course in price_filtered_courses:
    print(f"{course['title']} - Price: ${course['price']}")


# In[65]:


#Get users who joined in the last 6 months
from datetime import datetime, timedelta

# calculate date 6 months ago
six_months_ago = datetime.utcnow() - timedelta(days=6*30)  # approx 6 months

recent_users = users_collection.find({
    "dateJoined": {"$gte": six_months_ago}
})

print("Users who joined in the last 6 months:")
for user in recent_users:
    print(f"{user['firstName']} {user['lastName']} - Joined: {user['dateJoined']}")


# In[66]:


#Find courses that have specific tags using $in operator
# tags to search for
search_tags = ["cloud", "python"]

courses_with_tags = courses_collection.find({
    "tags": {"$in": search_tags}
})

print("Courses with specific tags:")
for course in courses_with_tags:
    print(f"{course['title']} - Tags: {course['tags']}")


# In[67]:


#Retrieve assignments with due dates in the next week
# calculate dates
today = datetime.utcnow()
one_week_later = today + timedelta(days=7)

assignments_due_next_week = db["assignments"].find({
    "dueDate": {"$gte": today, "$lte": one_week_later}
})

print("Assignments due in the next week:")
for assignment in assignments_due_next_week:
    print(f"{assignment['title']} - Due: {assignment['dueDate']}")


# In[68]:


#Course Enrollment Statistics: Count total enrollments per course

pipeline = [
    {
        "$group": {
            "_id": "$courseId",      # group by courseId
            "totalEnrollments": {"$sum": 1}  # count number of enrollments
        }
    },
    {
        "$lookup": {
            "from": "courses",
            "localField": "_id",
            "foreignField": "courseId",
            "as": "course_info"
        }
    },
    {
        "$unwind": "$course_info"
    },
    {
        "$project": {
            "_id": 0,
            "courseTitle": "$course_info.title",
            "totalEnrollments": 1
        }
    }
]

result = db["enrollments"].aggregate(pipeline)

print("Total enrollments per course:")
for doc in result:
    print(f"{doc['courseTitle']}: {doc['totalEnrollments']} students")


# In[69]:


#Calculate average course rating

pipeline = [
    {
        "$project": {
            "title": 1,
            "averageRating": {"$avg": "$ratings"}  # compute average of ratings array
        }
    }
]

result = courses_collection.aggregate(pipeline)

print("Average course ratings:")
for doc in result:
    print(f"{doc['title']}: {doc['averageRating']}")


# In[71]:


#Group courses by category
pipeline = [
    {
        "$group": {
            "_id": "$category",
            "courses": {"$push": "$title"}, 
            "count": {"$sum": 1}           
        }
    }
]

result = courses_collection.aggregate(pipeline)

print("Courses grouped by category:")
for doc in result:
    print(f"Category: {doc['_id']} ({doc['count']} courses)")
    print("Courses:", ", ".join(doc["courses"]))
    print("-" * 50)


# In[77]:


#Average grade per student

pipeline = [
    {
        "$group": {
            "_id": "$studentId",   # group by studentId
            "averageGrade": {"$avg": "$grade"}  # average of grade
        }
    }
]

result = db["submissions"].aggregate(pipeline)

print("Average grades raw output:")
for doc in result:
    print(doc)


# In[73]:


#Completion rate by course

pipeline = [
    {
        "$lookup": {
            "from": "assignments",
            "localField": "courseId",
            "foreignField": "courseId",
            "as": "course_assignments"
        }
    },
    {
        "$lookup": {
            "from": "submissions",
            "localField": "courseId",
            "foreignField": "courseId",
            "as": "course_submissions"
        }
    },
    {
        "$group": {
            "_id": "$courseId",
            "totalAssignments": {"$first": {"$size": "$course_assignments"}},
            "totalSubmissions": {"$first": {"$size": "$course_submissions"}}
        }
    },
    {
        "$project": {
            "_id": 0,
            "courseId": "$_id",
            "completionRate": {
                "$cond": [
                    {"$eq": ["$totalAssignments", 0]},
                    0,
                    {"$divide": ["$totalSubmissions", "$totalAssignments"]}
                ]
            }
        }
    }
]

result = db["courses"].aggregate(pipeline)

print("Completion rate by course:")
for doc in result:
    print(f"Course ID: {doc['courseId']}, Completion Rate: {doc['completionRate']:.2%}")


# In[76]:


#Top performing students
pipeline = [
    {
        "$group": {
            "_id": "$studentId",           # studentId from submissions
            "averageGrade": {"$avg": "$grade"}
        }
    },
    {
        "$lookup": {
            "from": "users",
            "localField": "_id",           # studentId from group
            "foreignField": "userId",      # userId in users collection
            "as": "student_info"
        }
    },
    {"$unwind": "$student_info"},
    {
        "$project": {
            "_id": 0,
            "studentName": {"$concat": ["$student_info.firstName", " ", "$student_info.lastName"]},
            "averageGrade": 1
        }
    },
    {"$sort": {"averageGrade": -1}},
    {"$limit": 5}
]

result = db["submissions"].aggregate(pipeline)

print("Top-performing students:")
for doc in result:
    print(f"{doc['studentName']}: {doc['averageGrade']:.2f}")


# In[79]:


pipeline = [
    {
        "$lookup": {
            "from": "enrollments",
            "localField": "courseId",
            "foreignField": "courseId",
            "as": "course_enrollments"
        }
    },
    {
        "$project": {
            "instructorId": 1,
            "courseTitle": "$title",
            "studentsCount": {"$size": "$course_enrollments"}
        }
    },
    {
        "$group": {
            "_id": "$instructorId",
            "totalStudents": {"$sum": "$studentsCount"}
        }
    },
    {
        "$lookup": {
            "from": "users",
            "localField": "_id",
            "foreignField": "userId", 
            "as": "instructor_info"
        }
    },
    {"$unwind": "$instructor_info"},
    {
        "$project": {
            "_id": 0,
            "instructorName": {"$concat": ["$instructor_info.firstName", " ", "$instructor_info.lastName"]},
            "totalStudents": 1
        }
    }
]

result = courses_collection.aggregate(pipeline)

print("Total students taught by each instructor:")
for doc in result:
    print(f"{doc['instructorName']}: {doc['totalStudents']} students")


# In[81]:


#Average course rating per instructor

pipeline = [
    {
        "$project": {
            "instructorId": 1,
            "ratings": {"$ifNull": ["$ratings", []]}  # replace null with empty array
        }
    },
    {
        "$group": {
            "_id": "$instructorId",
            "averageRating": {"$avg": {"$avg": "$ratings"}}  # averages across all courses
        }
    },
    {
        "$lookup": {
            "from": "users",
            "localField": "_id",
            "foreignField": "userId",
            "as": "instructor_info"
        }
    },
    {"$unwind": "$instructor_info"},
    {
        "$project": {
            "_id": 0,
            "instructorName": {"$concat": ["$instructor_info.firstName", " ", "$instructor_info.lastName"]},
            "averageRating": {"$ifNull": ["$averageRating", 0]}  # set 0 if None
        }
    }
]

result = courses_collection.aggregate(pipeline)

print("Average course rating per instructor:")
for doc in result:
    print(f"{doc['instructorName']}: {doc['averageRating']:.2f}")


# In[82]:


#Revenue generated per instructor
pipeline = [
    {
        "$lookup": {
            "from": "enrollments",
            "localField": "courseId",
            "foreignField": "courseId",
            "as": "course_enrollments"
        }
    },
    {
        "$project": {
            "instructorId": 1,
            "courseRevenue": {"$multiply": [{"$size": "$course_enrollments"}, "$price"]}
        }
    },
    {
        "$group": {
            "_id": "$instructorId",
            "totalRevenue": {"$sum": "$courseRevenue"}
        }
    },
    {
        "$lookup": {
            "from": "users",
            "localField": "_id",
            "foreignField": "userId",
            "as": "instructor_info"
        }
    },
    {"$unwind": "$instructor_info"},
    {
        "$project": {
            "_id": 0,
            "instructorName": {"$concat": ["$instructor_info.firstName", " ", "$instructor_info.lastName"]},
            "totalRevenue": 1
        }
    }
]

result = courses_collection.aggregate(pipeline)

print("Revenue generated per instructor:")
for doc in result:
    print(f"{doc['instructorName']}: ${doc['totalRevenue']:.2f}")


# In[85]:


#monthly enrollment trend
pipeline = [
    {
        "$match": {
            "dateEnrolled": {"$ne": None}  # exclude documents with no date
        }
    },
    {
        "$project": {
            "year": {"$year": "$dateEnrolled"},
            "month": {"$month": "$dateEnrolled"}
        }
    },
    {
        "$group": {
            "_id": {"year": "$year", "month": "$month"},
            "totalEnrollments": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id.year": 1, "_id.month": 1}  # chronological order
    }
]

result = db["enrollments"].aggregate(pipeline)

print(" Monthly enrollment trends:")
for doc in result:
    print(f"{doc['_id']['year']}-{doc['_id']['month']:02d}: {doc['totalEnrollments']} enrollments")


# In[86]:


#most popular course categories
pipeline = [
    {
        "$lookup": {
            "from": "enrollments",
            "localField": "courseId",
            "foreignField": "courseId",
            "as": "course_enrollments"
        }
    },
    {
        "$project": {
            "category": 1,
            "enrollmentsCount": {"$size": "$course_enrollments"}
        }
    },
    {
        "$group": {
            "_id": "$category",
            "totalEnrollments": {"$sum": "$enrollmentsCount"}
        }
    },
    {"$sort": {"totalEnrollments": -1}}
]

result = courses_collection.aggregate(pipeline)

print("Most popular course categories:")
for doc in result:
    print(f"{doc['_id']}: {doc['totalEnrollments']} enrollments")


# In[87]:


#student engagement metrcis 
pipeline = [
    {
        "$group": {
            "_id": "$studentId",
            "assignmentsSubmitted": {"$sum": 1}
        }
    },
    {
        "$lookup": {
            "from": "users",
            "localField": "_id",
            "foreignField": "userId",
            "as": "student_info"
        }
    },
    {"$unwind": "$student_info"},
    {
        "$project": {
            "_id": 0,
            "studentName": {"$concat": ["$student_info.firstName", " ", "$student_info.lastName"]},
            "assignmentsSubmitted": 1
        }
    },
    {"$sort": {"assignmentsSubmitted": -1}}
]

result = db["submissions"].aggregate(pipeline)

print("Student engagement metrics (assignments submitted):")
for doc in result:
    print(f"{doc['studentName']}: {doc['assignmentsSubmitted']} submissions")


# In[89]:


#index for user email lookup
# users_collection is already defined
users_collection.create_index("email", unique=True)
print("Index created on users.email (unique)")


# In[90]:


#Index for course search by title and category
courses_collection.create_index([("title", "text"), ("category", 1)])
print("Text index on courses.title + index on courses.category")


# In[91]:


#index for assignment queries by due date
assignments_collection.create_index("dueDate")
print("Index created on assignments.dueDate")


# In[92]:


#index for enrollment queries by student and course
enrollments_collection.create_index([("studentId", 1), ("courseId", 1)])
print("Compound index on enrollments.studentId + enrollments.courseId")


# In[94]:


#Analyze query performance using explain() method in PyMongo
from pprint import pprint

query = {"email": "ellis@example.com"}
explain_result = users_collection.find(query).explain()
print("Query plan for finding user by email:")
pprint(explain_result)


# In[96]:


#Analyze query performance using explain() method in PyMongo
from datetime import datetime

today = datetime.utcnow()
query = {"dueDate": {"$gte": today}}
explain_result = assignments_collection.find(query).explain()
print("Query plan for upcoming assignments:")
pprint(explain_result)


# In[97]:


from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["eduhub_db"]

# Drop collection if it exists (optional for testing)
db.users.drop()

# Create collection with JSON schema validation
db.create_collection(
    "users",
    validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["userId", "email", "firstName", "lastName", "role", "isActive"],
            "properties": {
                "userId": {"bsonType": "string"},
                "email": {
                    "bsonType": "string",
                    "pattern": r"^[\w\.-]+@[\w\.-]+\.\w+$",  # simple email regex
                    "description": "must be a valid email"
                },
                "firstName": {"bsonType": "string"},
                "lastName": {"bsonType": "string"},
                "role": {
                    "enum": ["student", "instructor"],
                    "description": "can only be student or instructor"
                },
                "profile": {
                    "bsonType": "object",
                    "properties": {
                        "bio": {"bsonType": "string"},
                        "avatar": {"bsonType": "string"},
                        "skills": {
                            "bsonType": "array",
                            "items": {"bsonType": "string"}
                        }
                    }
                },
                "isActive": {"bsonType": "bool"}
            }
        }
    }
)

print(" Users collection created with validation rules")


# In[98]:


db.courses.drop()

db.create_collection(
    "courses",
    validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["courseId", "title", "instructorId", "level", "isPublished"],
            "properties": {
                "courseId": {"bsonType": "string"},
                "title": {"bsonType": "string"},
                "description": {"bsonType": "string"},
                "instructorId": {"bsonType": "string"},
                "category": {"bsonType": "string"},
                "level": {
                    "enum": ["beginner", "intermediate", "advanced"]
                },
                "duration": {"bsonType": "int"},
                "price": {"bsonType": "double"},
                "tags": {
                    "bsonType": "array",
                    "items": {"bsonType": "string"}
                },
                "createdAt": {"bsonType": "date"},
                "updatedAt": {"bsonType": "date"},
                "isPublished": {"bsonType": "bool"}
            }
        }
    }
)

print("Courses collection created with validation rules")


# In[99]:


from pymongo.errors import WriteError

new_user = {
    "userId": "u1001",
    "email": "invalid_email",  # invalid
    "firstName": "Ellis",
    "lastName": "Ologun",
    "role": "student",
    "isActive": True
}

try:
    db.users.insert_one(new_user)
except WriteError as e:
    print("Failed to insert user due to validation error:")
    print(e)


# In[100]:


#duplicate key errors
from pymongo.errors import DuplicateKeyError

duplicate_user = {
    "userId": "u1001",  # already exists
    "email": "ellis@example.com",  # already exists
    "firstName": "Ellis",
    "lastName": "Ologun",
    "role": "student",
    "isActive": True
}

try:
    db.users.insert_one(duplicate_user)
except DuplicateKeyError as e:
    print("Duplicate key error detected:")
    print(e)


# In[101]:


#Invalid data typr insertions
from pymongo.errors import WriteError

invalid_user = {
    "userId": "u1002",
    "email": "ellis2@example.com",
    "firstName": "Ellis",
    "lastName": "Ologun",
    "role": "student",
    "isActive": "True" 
}

try:
    db.users.insert_one(invalid_user)
except WriteError as e:
    print("Invalid data type error detected:")
    print(e)


# In[102]:


#missing required fields 
missing_field_user = {
    "userId": "u1003",
    "email": "ellis3@example.com",
    # missing firstName and lastName
    "role": "student",
    "isActive": True
}

try:
    db.users.insert_one(missing_field_user)
except WriteError as e:
    print("Missing required fields error detected:")
    print(e)


# In[107]:


#Bonus Design a data archiving strategy for old enrollments
from datetime import datetime, timedelta

# Example: enrollments older than 1 year
one_year_ago = datetime.utcnow() - timedelta(days=365)

old_enrollments = list(enrollments_collection.find({
    "enrolledAt": {"$lt": one_year_ago}
}))

print(f"Found {len(old_enrollments)} old enrollments to archive.")

enrollments_archive_collection = db["enrollments_archive"]

if old_enrollments:
    # Insert into archive
    enrollments_archive_collection.insert_many(old_enrollments)
    print(f"Archived {len(old_enrollments)} enrollments.")

    # Delete from main collection
    ids_to_delete = [e["_id"] for e in old_enrollments]
    enrollments_collection.delete_many({"_id": {"$in": ids_to_delete}})
    print(f"Removed {len(old_enrollments)} old enrollments from active collection.")
else:
    print("⚠ No old enrollments found for archiving.")


    

enrollments_archive_collection.create_index("studentId")
enrollments_archive_collection.create_index("courseId")
enrollments_archive_collection.create_index("enrolledAt")




# In[118]:


from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['eduhub_db']

users = list(db['users'].find())
print(f"Total users: {len(users)}")
for u in users[:5]:  # print first 5 users
    print(u)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




